using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Utils;

internal sealed class InMemoryTcpConnectionFactory : ITcpConnectionFactory
{
    private readonly List<ConnectionLease> _established = [];

    public IReadOnlyList<ConnectionLease> EstablishedLeases => _established;

    public Task<ConnectionLease> EstablishAsync(TransportOptions options, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var state = new ClientState(Stream.Null);
        var cts = new CancellationTokenSource();
        var handle = new ConnectionHandle(state.OutboundWriter, state.InboundReader, cts.Token);
        var lease = new ConnectionLease(handle, state, cts, ConnectionInfo.None);

        _established.Add(lease);
        return Task.FromResult(lease);
    }
}
