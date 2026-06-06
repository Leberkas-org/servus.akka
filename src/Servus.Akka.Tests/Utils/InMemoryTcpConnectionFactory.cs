using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Utils;

internal sealed class InMemoryTcpConnectionFactory(TimeProvider? timeProvider = null) : ITcpConnectionFactory
{
    private readonly List<ConnectionLease> _established = [];

    public IReadOnlyList<ConnectionLease> EstablishedLeases => _established;

    public Task<ConnectionLease> EstablishAsync(TransportOptions options, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var connection = SocketPipeConnection.Create(Stream.Null);
        var leaseTracker = new LeaseTracker(16);
        var cts = new CancellationTokenSource();
        var lease = new ConnectionLease(connection, leaseTracker, cts, ConnectionInfo.None, timeProvider);

        _established.Add(lease);
        return Task.FromResult(lease);
    }
}
