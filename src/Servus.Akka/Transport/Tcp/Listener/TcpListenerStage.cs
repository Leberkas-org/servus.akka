using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using Akka;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;

namespace Servus.Akka.Transport.Tcp.Listener;

internal sealed record TcpClientAccepted(TcpClient Client);

internal sealed record TcpAcceptFailed(Exception Error);

internal sealed record TcpConnectionReady(Flow<ITransportOutbound, ITransportInbound, NotUsed> Flow);

internal sealed record TcpConnectionInitFailed(Exception Error);

internal sealed class TcpListenerStage
    : GraphStageWithMaterializedValue<SourceShape<Flow<ITransportOutbound, ITransportInbound, NotUsed>>, Task<int>>
{
    private readonly TcpListenerOptions _options;

    private readonly Outlet<Flow<ITransportOutbound, ITransportInbound, NotUsed>> _out =
        new("TcpListener.Out");

    public override SourceShape<Flow<ITransportOutbound, ITransportInbound, NotUsed>> Shape { get; }

    public TcpListenerStage(TcpListenerOptions options)
    {
        _options = options;
        Shape = new SourceShape<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(_out);
    }

    public override ILogicAndMaterializedValue<Task<int>> CreateLogicAndMaterializedValue(
        Attributes inheritedAttributes)
    {
        var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        return new LogicAndMaterializedValue<Task<int>>(new Logic(this, tcs), tcs.Task);
    }

    [ExcludeFromCodeCoverage]
    private sealed class Logic : GraphStageLogic
    {
        private readonly TcpListenerStage _stage;
        private readonly TaskCompletionSource<int> _boundSignal;
        private readonly Queue<Flow<ITransportOutbound, ITransportInbound, NotUsed>> _pendingConnections = new();
        private TcpListener? _listener;
        private IActorRef _self = null!;
        private CancellationTokenSource? _cts;

        public Logic(TcpListenerStage stage, TaskCompletionSource<int> boundSignal) : base(stage.Shape)
        {
            _stage = stage;
            _boundSignal = boundSignal;

            SetHandler(stage._out, onPull: TryPush);
        }

        public override void PreStart()
        {
            var stageActor = GetStageActor(OnReceive);
            _self = stageActor.Ref;
            _cts = new CancellationTokenSource();

            var address = IPAddress.TryParse(_stage._options.Host, out var ip)
                ? ip
                : IPAddress.Any;

            _listener = new TcpListener(address, _stage._options.Port);

            if (_stage._options.ReuseAddress)
            {
                _listener.Server.SetSocketOption(
                    SocketOptionLevel.Socket,
                    SocketOptionName.ReuseAddress,
                    true);
            }

            _listener.Start(_stage._options.Backlog);
            var actualPort = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _boundSignal.TrySetResult(actualPort);
            _ = AcceptLoopAsync(_listener, _self, _cts.Token);
        }

        public override void PostStop()
        {
            _cts?.Cancel();
            _cts?.Dispose();
            _cts = null;

            _listener?.Stop();
            _listener = null;

            while (_pendingConnections.TryDequeue(out _))
            {
            }
        }

        private static async Task AcceptLoopAsync(TcpListener listener, IActorRef self, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var client = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                    self.Tell(new TcpClientAccepted(client));
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    self.Tell(new TcpAcceptFailed(ex));
                    return;
                }
            }
        }

        private void OnReceive((IActorRef sender, object message) args)
        {
            switch (args.message)
            {
                case TcpClientAccepted accepted:
                    OnClientAccepted(accepted.Client);
                    break;
                case TcpAcceptFailed failed:
                    OnAcceptError(failed.Error);
                    break;
                case TcpConnectionReady ready:
                    _pendingConnections.Enqueue(ready.Flow);
                    TryPush();
                    break;
                case TcpConnectionInitFailed failed:
                    Log.Warning(failed.Error, "Failed to initialize accepted connection");
                    break;
            }
        }

        private void OnClientAccepted(TcpClient client)
        {
            _ = InitializeConnectionAsync(client);
        }

        private async Task InitializeConnectionAsync(TcpClient client)
        {
            TlsConnectionResult tlsResult;
            try
            {
                if (_stage._options.NoDelay)
                {
                    client.NoDelay = true;
                }

                if (_stage._options.SocketSendBufferSize is { } sendBuf)
                {
                    client.SendBufferSize = sendBuf;
                }

                if (_stage._options.SocketReceiveBufferSize is { } recvBuf)
                {
                    client.ReceiveBufferSize = recvBuf;
                }

                tlsResult = await GetTlsStreamAsync(client);
            }
            catch (Exception ex)
            {
                client.Dispose();
                _self.Tell(new TcpConnectionInitFailed(ex));
                return;
            }

            var localEndPoint = client.Client.LocalEndPoint!;
            var remoteEndPoint = client.Client.RemoteEndPoint!;

            var connectionInfo = new ConnectionInfo(
                localEndPoint,
                remoteEndPoint,
                tlsResult.Security is not null ? TransportProtocol.Tls : TransportProtocol.Tcp,
                tlsResult.Security);

            var connectionFlow = Flow.FromGraph(
                new TcpServerConnectionStage(
                    tlsResult.Stream,
                    connectionInfo,
                    tlsResult.SslStream,
                    tlsResult.AllowDelayedNegotiation));

            _self.Tell(new TcpConnectionReady(connectionFlow));
        }

        private void TryPush()
        {
            if (IsAvailable(_stage._out) && _pendingConnections.TryDequeue(out var flow))
            {
                Push(_stage._out, flow);
            }
        }

        private async Task<TlsConnectionResult> GetTlsStreamAsync(TcpClient client)
        {
            var options = _stage._options;

            if (options.ServerCertificate is null && options.ServerCertificateSelector is null)
            {
                return new TlsConnectionResult(client.GetStream(), Security: null, SslStream: null,
                    AllowDelayedNegotiation: false);
            }

            return await AuthenticateWithOptionsAsync(client, options);
        }

        private async Task<TlsConnectionResult> AuthenticateWithOptionsAsync(TcpClient client,
            TcpListenerOptions options)
        {
            var sslStream = new SslStream(
                client.GetStream(),
                leaveInnerStreamOpen: false,
                options.ClientCertificateValidationCallback);

            string? hostname = null;
            var clientCertRequired = options.ClientCertificateMode is ClientCertificateMode.RequireCertificate
                or ClientCertificateMode.AllowCertificate;

            SslServerAuthenticationOptions authOptions;

            if (options.ServerCertificateSelector is { } selector)
            {
                authOptions = new SslServerAuthenticationOptions
                {
                    ServerCertificateSelectionCallback = (_, host) =>
                    {
                        hostname = host;
                        return selector(host) ?? options.ServerCertificate!;
                    },
                    ClientCertificateRequired = clientCertRequired,
                    EnabledSslProtocols = options.EnabledSslProtocols,
                    ApplicationProtocols = options.ApplicationProtocols
                };
            }
            else
            {
                authOptions = new SslServerAuthenticationOptions
                {
                    ServerCertificate = options.ServerCertificate,
                    ClientCertificateRequired = clientCertRequired,
                    EnabledSslProtocols = options.EnabledSslProtocols,
                    ApplicationProtocols = options.ApplicationProtocols
                };
            }

            await sslStream.AuthenticateAsServerAsync(authOptions, CancellationToken.None)
                .WaitAsync(options.HandshakeTimeout, CancellationToken.None);

            if (hostname is null && sslStream.TargetHostName is { Length: > 0 } targetHost)
            {
                hostname = targetHost;
            }

            var security = CaptureSecurityInfo(sslStream, hostname);
            var allowDelayed = options.ClientCertificateMode is ClientCertificateMode.DelayCertificate;
            return new TlsConnectionResult(sslStream, security, sslStream, allowDelayed);
        }

        private static SecurityInfo CaptureSecurityInfo(SslStream sslStream, string? hostname)
        {
            return new SecurityInfo(
                sslStream.SslProtocol,
                sslStream.NegotiatedApplicationProtocol,
                sslStream.NegotiatedCipherSuite,
                hostname);
        }

        private void OnAcceptError(Exception ex)
        {
            if (ex is ObjectDisposedException or OperationCanceledException)
            {
                return;
            }

            Log.Error(ex, "TCP listener accept failed");
            FailStage(ex);
        }
    }
}