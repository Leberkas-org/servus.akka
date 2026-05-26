using System.Threading.Channels;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class ConnectionHandleSpec
{
    private static (ConnectionHandle Handle, Channel<TransportBuffer> Outbound, Channel<TransportBuffer> Inbound, CancellationTokenSource Cts) CreateHandle()
    {
        var outbound = Channel.CreateUnbounded<TransportBuffer>();
        var inbound = Channel.CreateUnbounded<TransportBuffer>();
        var cts = new CancellationTokenSource();
        var handle = new ConnectionHandle(outbound.Writer, inbound.Reader, cts.Token);
        return (handle, outbound, inbound, cts);
    }

    [Fact(Timeout = 5000)]
    public void Write_should_send_buffer_to_outbound_channel()
    {
        var (handle, outbound, _, cts) = CreateHandle();
        var buf = TransportBuffer.Rent(3);
        buf.FullMemory.Span[0] = 0xAA;
        buf.Length = 1;

        handle.Write(buf);

        Assert.True(outbound.Reader.TryRead(out var received));
        Assert.Equal(0xAA, received.Span[0]);
        received.Dispose();
        cts.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void TryRead_should_return_false_when_empty()
    {
        var (handle, _, _, cts) = CreateHandle();

        Assert.False(handle.TryRead(out _));
        cts.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void TryRead_should_return_buffer_from_inbound_channel()
    {
        var (handle, _, inbound, cts) = CreateHandle();
        var buf = TransportBuffer.Rent(3);
        buf.FullMemory.Span[0] = 0xBB;
        buf.Length = 1;
        inbound.Writer.TryWrite(buf);

        Assert.True(handle.TryRead(out var received));
        Assert.Equal(0xBB, received!.Span[0]);
        received.Dispose();
        cts.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void SignalClose_should_complete_outbound_writer()
    {
        var (handle, outbound, _, cts) = CreateHandle();

        handle.SignalClose();

        Assert.True(outbound.Reader.Completion.IsCompleted);
        cts.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void IsCancelled_should_be_false_initially()
    {
        var (handle, _, _, cts) = CreateHandle();

        Assert.False(handle.IsCancelled);
        cts.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void IsCancelled_should_be_true_after_token_cancelled()
    {
        var (handle, _, _, cts) = CreateHandle();

        cts.Cancel();

        Assert.True(handle.IsCancelled);
        cts.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Write_should_dispose_buffer_when_channel_is_full()
    {
        var outbound = Channel.CreateBounded<TransportBuffer>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite
        });
        var inbound = Channel.CreateUnbounded<TransportBuffer>();
        var cts = new CancellationTokenSource();
        var handle = new ConnectionHandle(outbound.Writer, inbound.Reader, cts.Token);

        var buf1 = TransportBuffer.Rent(1);
        buf1.Length = 1;
        handle.Write(buf1);

        outbound.Writer.TryComplete();

        var buf2 = TransportBuffer.Rent(1);
        buf2.Length = 1;
        handle.Write(buf2);

        cts.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Equals_should_return_true_for_same_instance()
    {
        var (handle, _, _, cts) = CreateHandle();

        Assert.True(handle.Equals(handle));
        cts.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Equals_should_return_false_for_different_instance()
    {
        var (handle1, _, _, cts1) = CreateHandle();
        var (handle2, _, _, cts2) = CreateHandle();

        Assert.NotEqual(handle1, handle2);
        cts1.Dispose();
        cts2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void GetHashCode_should_be_consistent()
    {
        var (handle, _, _, cts) = CreateHandle();

        var hash1 = handle.GetHashCode();
        var hash2 = handle.GetHashCode();

        Assert.Equal(hash1, hash2);
        cts.Dispose();
    }
}
