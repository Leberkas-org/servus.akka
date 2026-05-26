using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using Akka;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;

namespace Servus.Akka.Transport.Quic.Listener;

internal sealed record QuicConnectionAccepted(QuicConnection Connection);

internal sealed record QuicAcceptFailed(Exception Error);

internal sealed record QuicListenerBound(QuicListener Listener);

internal sealed class QuicListenerStage
    : GraphStageWithMaterializedValue<SourceShape<Flow<ITransportOutbound, ITransportInbound, NotUsed>>, Task>
{
    private readonly QuicListenerOptions _options;

    private readonly Outlet<Flow<ITransportOutbound, ITransportInbound, NotUsed>> _out =
        new("QuicListener.Out");

    public override SourceShape<Flow<ITransportOutbound, ITransportInbound, NotUsed>> Shape { get; }

    public QuicListenerStage(QuicListenerOptions options)
    {
        _options = options;
        Shape = new SourceShape<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(_out);
    }

    public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(
        Attributes inheritedAttributes)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        return new LogicAndMaterializedValue<Task>(new Logic(this, tcs), tcs.Task);
    }

    [ExcludeFromCodeCoverage]
    private sealed class Logic : GraphStageLogic
    {
        private readonly QuicListenerStage _stage;
        private readonly TaskCompletionSource _boundSignal;
        private readonly Queue<Flow<ITransportOutbound, ITransportInbound, NotUsed>> _pendingConnections = new();
        private QuicListener? _listener;
        private IActorRef _self = null!;
        private CancellationTokenSource? _cts;

        public Logic(QuicListenerStage stage, TaskCompletionSource boundSignal) : base(stage.Shape)
        {
            _stage = stage;
            _boundSignal = boundSignal;

            SetHandler(stage._out, onPull: () => TryPush());
        }

        public override void PreStart()
        {
            var stageActor = GetStageActor(OnReceive);
            _self = stageActor.Ref;
            _cts = new CancellationTokenSource();

            BindAsync(_cts.Token)
                .PipeTo(_self,
                    success: listener => new QuicListenerBound(listener),
                    failure: ex => new QuicAcceptFailed(ex));
        }

        public override void PostStop()
        {
            _cts?.Cancel();
            _cts?.Dispose();
            _cts = null;

            if (_listener is not null)
            {
                _ = _listener.DisposeAsync();
                _listener = null;
            }

            while (_pendingConnections.TryDequeue(out _))
            {
            }
        }

        private async Task<QuicListener> BindAsync(CancellationToken ct)
        {
            var opts = _stage._options;
            var address = IPAddress.TryParse(opts.Host, out var ip)
                ? ip
                : IPAddress.Any;

            var nativeListenerOptions = new System.Net.Quic.QuicListenerOptions
            {
                ListenEndPoint = new IPEndPoint(address, opts.Port),
                ApplicationProtocols = opts.ApplicationProtocols,
                ConnectionOptionsCallback = (_, _, _) =>
                {
                    var serverOptions = new QuicServerConnectionOptions
                    {
                        DefaultStreamErrorCode = 0x0100,
                        DefaultCloseErrorCode = 0x0100,
                        MaxInboundBidirectionalStreams = opts.MaxInboundBidirectionalStreams,
                        MaxInboundUnidirectionalStreams = opts.MaxInboundUnidirectionalStreams,
                        IdleTimeout = opts.IdleTimeout,
                        ServerAuthenticationOptions = new SslServerAuthenticationOptions
                        {
                            ServerCertificate = opts.ServerCertificate,
                            ApplicationProtocols = opts.ApplicationProtocols,
                            EnabledSslProtocols = opts.EnabledSslProtocols,
                            RemoteCertificateValidationCallback = opts.ClientCertificateValidationCallback
                        }
                    };
                    return ValueTask.FromResult(serverOptions);
                }
            };

            return await QuicListener.ListenAsync(nativeListenerOptions, ct).ConfigureAwait(false);
        }

        private static async Task AcceptLoopAsync(QuicListener listener, IActorRef self, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var connection = await listener.AcceptConnectionAsync(ct).ConfigureAwait(false);
                    self.Tell(new QuicConnectionAccepted(connection));
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    self.Tell(new QuicAcceptFailed(ex));
                    return;
                }
            }
        }

        private void OnReceive((IActorRef sender, object message) args)
        {
            switch (args.message)
            {
                case QuicListenerBound bound:
                    _listener = bound.Listener;
                    _boundSignal.TrySetResult();
                    _ = AcceptLoopAsync(_listener, _self, _cts!.Token);
                    break;
                case QuicConnectionAccepted accepted:
                    OnConnectionAccepted(accepted.Connection);
                    break;
                case QuicAcceptFailed failed:
                    OnAcceptError(failed.Error);
                    break;
            }
        }

        private void OnConnectionAccepted(QuicConnection connection)
        {
            SecurityInfo? security = connection.NegotiatedApplicationProtocol.Protocol.Length > 0
                ? new SecurityInfo(
                    System.Security.Authentication.SslProtocols.None,
                    connection.NegotiatedApplicationProtocol)
                : null;

            var connectionInfo = new ConnectionInfo(
                connection.LocalEndPoint,
                connection.RemoteEndPoint,
                TransportProtocol.Quic,
                security);

            var handle = new QuicConnectionHandle(
                openStream: async (direction, token) =>
                {
                    var streamType = direction == StreamDirection.Bidirectional
                        ? QuicStreamType.Bidirectional
                        : QuicStreamType.Unidirectional;
                    var stream = await connection.OpenOutboundStreamAsync(streamType, token).ConfigureAwait(false);
                    return (stream, stream.Id);
                },
                acceptInboundStream: async token =>
                {
                    try
                    {
                        var stream = await connection.AcceptInboundStreamAsync(token).ConfigureAwait(false);
                        return (stream, stream.Id);
                    }
                    catch (OperationCanceledException)
                    {
                        return null;
                    }
                    catch
                    {
                        return null;
                    }
                },
                getLocalEndPoint: () => connection.LocalEndPoint,
                getRemoteEndPoint: () => connection.RemoteEndPoint,
                dispose: () => connection.DisposeAsync());

            var connectionFlow = Flow.FromGraph(
                new QuicServerConnectionStage(handle, connectionInfo));

            _pendingConnections.Enqueue(connectionFlow);
            TryPush();
        }

        private void TryPush()
        {
            if (IsAvailable(_stage._out) && _pendingConnections.TryDequeue(out var flow))
            {
                Push(_stage._out, flow);
            }
        }

        private void OnAcceptError(Exception ex)
        {
            if (ex is ObjectDisposedException or OperationCanceledException)
            {
                return;
            }

            Log.Error(ex, "QUIC listener accept failed");
            FailStage(ex);
        }
    }
}
