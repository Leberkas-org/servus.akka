using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;

namespace Servus.Akka.Tests.Transport.Quic;

[Collection("TransportBuffer")]
public sealed class StreamHandleSpec
{
    [Fact(Timeout = 5000)]
    public void Write_should_write_buffer_to_stream()
    {
        var ms = new MemoryStream();
        var handle = new StreamHandle(ms);

        var buffer = TransportBuffer.Rent(16);
        buffer.FullMemory.Span[0] = 0xAA;
        buffer.FullMemory.Span[1] = 0xBB;
        buffer.Length = 2;

        handle.Write(buffer);

        Assert.Equal(2, ms.Position);
        Assert.Equal(0xAA, ms.GetBuffer()[0]);
        Assert.Equal(0xBB, ms.GetBuffer()[1]);
    }

    [Fact(Timeout = 5000)]
    public async Task ReadAsync_should_read_from_stream()
    {
        var ms = new MemoryStream([0x01, 0x02, 0x03]);
        var handle = new StreamHandle(ms);

        var buf = new byte[16];
        var read = await handle.ReadAsync(buf, CancellationToken.None);

        Assert.Equal(3, read);
        Assert.Equal(0x01, buf[0]);
    }

    [Fact(Timeout = 5000)]
    public void CompleteWrites_should_not_throw()
    {
        var handle = new StreamHandle(Stream.Null);
        handle.CompleteWrites();
    }

    [Fact(Timeout = 5000)]
    public void Write_should_write_and_dispose_buffer()
    {
        var ms = new MemoryStream();
        var handle = new StreamHandle(ms);

        var buffer = TransportBuffer.Rent(16);
        buffer.FullMemory.Span[0] = 0x11;
        buffer.FullMemory.Span[1] = 0x22;
        buffer.FullMemory.Span[2] = 0x33;
        buffer.FullMemory.Span[3] = 0x44;
        buffer.Length = 4;

        handle.Write(buffer);

        Assert.Equal(4, ms.Length);
        Assert.Equal(0x11, ms.GetBuffer()[0]);
        Assert.Equal(0x22, ms.GetBuffer()[1]);
        Assert.Equal(0x33, ms.GetBuffer()[2]);
        Assert.Equal(0x44, ms.GetBuffer()[3]);

        Assert.Throws<NullReferenceException>(() => _ = buffer.Memory);
    }

    [Fact(Timeout = 5000)]
    public void Write_should_write_multiple_bytes_and_dispose_buffer()
    {
        var ms = new MemoryStream();
        var handle = new StreamHandle(ms);

        var buffer = TransportBuffer.Rent(16);
        buffer.FullMemory.Span[0] = 0x55;
        buffer.FullMemory.Span[1] = 0x66;
        buffer.FullMemory.Span[2] = 0x77;
        buffer.Length = 3;

        handle.Write(buffer);

        Assert.Equal(3, ms.Length);
        Assert.Equal(0x55, ms.GetBuffer()[0]);
        Assert.Equal(0x66, ms.GetBuffer()[1]);
        Assert.Equal(0x77, ms.GetBuffer()[2]);

        Assert.Throws<NullReferenceException>(() => _ = buffer.Memory);
    }

    [Fact(Timeout = 5000)]
    public void Abort_on_non_QuicStream_should_not_throw()
    {
        var ms = new MemoryStream();
        var handle = new StreamHandle(ms);

        handle.Abort(0);
        handle.Abort(42);
    }

    [Fact(Timeout = 5000)]
    public void CompleteWrites_on_non_QuicStream_should_not_throw()
    {
        var ms = new MemoryStream();
        var handle = new StreamHandle(ms);

        handle.CompleteWrites();
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_should_dispose_underlying_stream()
    {
        var ms = new MemoryStream([0x01, 0x02, 0x03]);
        var handle = new StreamHandle(ms);

        await handle.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => _ = ms.ReadByte());
    }

    [Fact(Timeout = 5000)]
    public async Task ReadAsync_should_return_zero_on_empty_stream()
    {
        var ms = new MemoryStream();
        ms.Position = 0;
        var handle = new StreamHandle(ms);

        var buf = new byte[16];
        var read = await handle.ReadAsync(buf, CancellationToken.None);

        Assert.Equal(0, read);
    }
}