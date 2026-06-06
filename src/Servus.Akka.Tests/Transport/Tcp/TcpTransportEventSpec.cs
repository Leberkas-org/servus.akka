using System.IO.Pipelines;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class TcpTransportEventSpec
{
    [Fact(Timeout = 5000)]
    public void LeaseAcquired_should_preserve_lease()
    {
        var connection = SocketPipeConnection.Create(Stream.Null);
        var leaseTracker = new LeaseTracker(16);
        var cts = new CancellationTokenSource();
        var lease = new ConnectionLease(connection, leaseTracker, cts, ConnectionInfo.None);

        var evt = new LeaseAcquired(lease);

        Assert.Same(lease, evt.Lease);
    }

    [Fact(Timeout = 5000)]
    public void AcquisitionFailed_should_preserve_error()
    {
        var ex = new IOException("test");
        var evt = new AcquisitionFailed(ex);

        Assert.Same(ex, evt.Error);
    }

    [Fact(Timeout = 5000)]
    public void PipeReadComplete_should_preserve_fields()
    {
        var result = new ReadResult(default, isCanceled: false, isCompleted: true);
        var evt = new PipeReadComplete(result, 5);

        Assert.Equal(5, evt.Gen);
        Assert.True(evt.Result.IsCompleted);
    }

    [Fact(Timeout = 5000)]
    public void PipeReadFailed_should_preserve_error()
    {
        var ex = new IOException("read error");
        var evt = new PipeReadFailed(ex, 3);

        Assert.Same(ex, evt.Error);
        Assert.Equal(3, evt.Gen);
    }
}
