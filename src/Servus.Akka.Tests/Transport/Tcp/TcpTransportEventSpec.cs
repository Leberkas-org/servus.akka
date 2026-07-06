using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class TcpTransportEventSpec
{
    [Fact(Timeout = 5000)]
    public void LeaseAcquired_should_preserve_lease()
    {
        var connection = new StreamConnection(Stream.Null, new TransportConnectionOptions());
        var cts = new CancellationTokenSource();
        var lease = new ConnectionLease(connection, cts, ConnectionInfo.None);

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
    public void ReadCompleted_should_preserve_fields()
    {
        var buffer = WireBuffer.Rent(3);
        buffer.Length = 3;
        var evt = new ReadCompleted(buffer, 5);

        Assert.Equal(5, evt.Gen);
        Assert.Same(buffer, evt.Buffer);

        buffer.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void ReadCompleted_null_buffer_should_preserve_gen()
    {
        var evt = new ReadCompleted(null, 7);

        Assert.Equal(7, evt.Gen);
        Assert.Null(evt.Buffer);
    }

    [Fact(Timeout = 5000)]
    public void ReadFailed_should_preserve_error()
    {
        var ex = new IOException("read error");
        var evt = new ReadFailed(ex, 3);

        Assert.Same(ex, evt.Error);
        Assert.Equal(3, evt.Gen);
    }
}
