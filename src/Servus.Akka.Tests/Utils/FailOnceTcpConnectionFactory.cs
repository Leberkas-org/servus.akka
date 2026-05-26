using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Utils;

internal sealed class FailOnceTcpConnectionFactory : ITcpConnectionFactory
{
    private int _callCount;

    public Task<ConnectionLease> EstablishAsync(TransportOptions options, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        if (Interlocked.Increment(ref _callCount) == 1)
        {
            return Task.FromException<ConnectionLease>(new IOException("Simulated first-call connection failure"));
        }

        var state = new ClientState(Stream.Null);
        var cts = new CancellationTokenSource();
        var handle = new ConnectionHandle(state.OutboundWriter, state.InboundReader, cts.Token);
        return Task.FromResult(new ConnectionLease(handle, state, cts, ConnectionInfo.None));
    }
}
