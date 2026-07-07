using System.Net;
using System.Net.Sockets;

namespace Servus.Akka.Transport.Tcp.Client;

internal sealed class TcpConnectionFactory : ITcpConnectionFactory
{
    public async Task<ConnectionLease> EstablishAsync(TransportOptions options, CancellationToken ct)
    {
        Stream stream;
        Socket? plaintextSocket = null;
        EndPoint? localEndPoint;
        EndPoint? remoteEndPoint;
        TransportProtocol protocol;
        SecurityInfo? security = null;

        if (options is TlsTransportOptions tlsOpts)
        {
            var tlsProvider = new TlsClientProvider(tlsOpts);
            stream = await tlsProvider.GetStreamAsync(ct).ConfigureAwait(false);
            localEndPoint = tlsProvider.LocalEndPoint;
            remoteEndPoint = tlsProvider.RemoteEndPoint;
            protocol = TransportProtocol.Tls;

            if (tlsProvider is { NegotiatedSslProtocol: { } sslProto, NegotiatedApplicationProtocol: { } appProto })
            {
                security = new SecurityInfo(sslProto, appProto);
            }
        }
        else if (options is TcpTransportOptions tcpOpts)
        {
            var tcpProvider = new TcpClientProvider(tcpOpts);
            stream = await tcpProvider.GetStreamAsync(ct).ConfigureAwait(false);
            plaintextSocket = tcpProvider.ConnectedSocket;
            localEndPoint = tcpProvider.LocalEndPoint;
            remoteEndPoint = tcpProvider.RemoteEndPoint;
            protocol = TransportProtocol.Tcp;
        }
        else
        {
            throw new ArgumentException($"Unsupported options type: {options.GetType()}", nameof(options));
        }

        var info = new ConnectionInfo(
            localEndPoint ?? new IPEndPoint(IPAddress.Any, 0),
            remoteEndPoint ?? new IPEndPoint(IPAddress.Any, 0),
            protocol,
            security);

        var connectionOptions = new TransportConnectionOptions
        {
            ReceiveBufferHint = options.ReceiveBufferHint,
            OutputHighWatermark = options.OutputPauseThreshold,
            OutputLowWatermark = options.OutputResumeThreshold,
        };

        IDuplexConnection connection = plaintextSocket is not null
            ? new RawSocketConnection(plaintextSocket, connectionOptions)
            : new StreamConnection(stream, connectionOptions, useZeroByteProbe: true);
        var cts = new CancellationTokenSource();
        var lease = new ConnectionLease(connection, cts, info, options: connectionOptions);

        return lease;
    }
}
