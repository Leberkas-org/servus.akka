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

        var connection = new StreamConnection(Stream.Null, new TransportConnectionOptions());
        var cts = new CancellationTokenSource();
        var lease = new ConnectionLease(connection, cts, ConnectionInfo.None, timeProvider);

        _established.Add(lease);
        return Task.FromResult(lease);
    }
}
