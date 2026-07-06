using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class TransportDataSpec
{
    private static WireBuffer CreateBuffer(params byte[] data)
    {
        var buf = WireBuffer.Rent(data.Length == 0 ? 1 : data.Length);
        data.CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        return buf;
    }

    [Fact]
    public void Rent_should_attach_buffer()
    {
        var buffer = CreateBuffer(1, 2, 3);

        var data = TransportData.Rent(buffer);

        Assert.Same(buffer, data.Buffer);

        buffer.Dispose();
        data.Return();
    }

    [Fact]
    public void Return_should_clear_buffer_reference()
    {
        var first = CreateBuffer(1);
        var data = TransportData.Rent(first);
        first.Dispose();

        data.Return();

        Assert.Null(data.Buffer);
    }

    [Fact]
    public void Rent_after_return_should_reattach_a_fresh_buffer()
    {
        var first = CreateBuffer(1);
        var data = TransportData.Rent(first);
        first.Dispose();
        data.Return();

        var second = CreateBuffer(2, 3);
        var next = TransportData.Rent(second);

        Assert.Same(second, next.Buffer);

        second.Dispose();
        next.Return();
    }
}
