using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class TcpTransportEventSpec
{
    [Fact(Timeout = 5000)]
    public void LeaseAcquired_should_preserve_lease()
    {
        var state = new ClientState(Stream.Null);
        var cts = new CancellationTokenSource();
        var handle = new ConnectionHandle(state.OutboundWriter, state.InboundReader, cts.Token);
        var lease = new ConnectionLease(handle, state, cts, ConnectionInfo.None);

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
    public void InboundBatch_should_preserve_fields()
    {
        var batch = new ITransportInbound[8];
        var evt = new InboundBatch(batch, 3, 7);

        Assert.Same(batch, evt.Batch);
        Assert.Equal(3, evt.Count);
        Assert.Equal(7, evt.Gen);
    }

    [Fact(Timeout = 5000)]
    public void InboundComplete_should_preserve_fields()
    {
        var evt = new InboundComplete(DisconnectReason.Error, 5);

        Assert.Equal(DisconnectReason.Error, evt.Reason);
        Assert.Equal(5, evt.Gen);
    }

    [Fact(Timeout = 5000)]
    public void InboundPumpFailed_should_preserve_error()
    {
        var ex = new IOException("pump error");
        var evt = new InboundPumpFailed(ex);

        Assert.Same(ex, evt.Error);
    }

    [Fact(Timeout = 5000)]
    public void OutboundWriteDone_should_implement_interface()
    {
        ITcpTransportEvent evt = new OutboundWriteDone(1);

        Assert.IsType<OutboundWriteDone>(evt);
    }

    [Fact(Timeout = 5000)]
    public void OutboundWriteFailed_should_preserve_error()
    {
        var ex = new IOException("write error");
        var evt = new OutboundWriteFailed(ex);

        Assert.Same(ex, evt.Error);
    }

    [Fact(Timeout = 5000)]
    public void InboundComplete_equality_should_compare_all_fields()
    {
        var a = new InboundComplete(DisconnectReason.Graceful, 1);
        var b = new InboundComplete(DisconnectReason.Graceful, 1);
        var c = new InboundComplete(DisconnectReason.Error, 1);
        var d = new InboundComplete(DisconnectReason.Graceful, 2);

        Assert.Equal(a, b);
        Assert.NotEqual(a, c);
        Assert.NotEqual(a, d);
    }

    [Fact(Timeout = 5000)]
    public void OutboundWriteDone_equality_should_compare_gen()
    {
        var a = new OutboundWriteDone(1);
        var b = new OutboundWriteDone(1);
        var c = new OutboundWriteDone(2);

        Assert.Equal(a, b);
        Assert.NotEqual(a, c);
    }
}
