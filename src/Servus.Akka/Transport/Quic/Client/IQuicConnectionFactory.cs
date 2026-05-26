namespace Servus.Akka.Transport.Quic.Client;

internal interface IQuicConnectionFactory
{
    Task<QuicConnectionLease> EstablishAsync(QuicTransportOptions options, CancellationToken ct);
}
