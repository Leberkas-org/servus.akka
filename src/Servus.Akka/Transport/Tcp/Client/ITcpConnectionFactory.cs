namespace Servus.Akka.Transport.Tcp.Client;

internal interface ITcpConnectionFactory
{
    Task<ConnectionLease> EstablishAsync(TransportOptions options, CancellationToken ct);
}