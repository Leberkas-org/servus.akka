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

        var connection = SocketPipeConnection.Create(Stream.Null);
        var leaseTracker = new LeaseTracker(16);
        var cts = new CancellationTokenSource();
        return Task.FromResult(new ConnectionLease(connection, leaseTracker, cts, ConnectionInfo.None));
    }
}
