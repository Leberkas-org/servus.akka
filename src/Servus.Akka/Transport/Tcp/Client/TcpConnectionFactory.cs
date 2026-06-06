using System.Net;

namespace Servus.Akka.Transport.Tcp.Client;

internal sealed class TcpConnectionFactory : ITcpConnectionFactory
{
    public async Task<ConnectionLease> EstablishAsync(TransportOptions options, CancellationToken ct)
    {
        Stream stream;
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

        var connection = SocketPipeConnection.Create(stream);
        var leaseTracker = new LeaseTracker(16);
        var cts = new CancellationTokenSource();
        var lease = new ConnectionLease(connection, leaseTracker, cts, info);

        return lease;
    }
}
