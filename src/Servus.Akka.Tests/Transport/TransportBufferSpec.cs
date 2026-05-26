using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

[CollectionDefinition("TransportBuffer", DisableParallelization = true)]
public class TransportBufferCollection;

[Collection("TransportBuffer")]
public sealed class TransportBufferSpec
{
    [Fact(Timeout = 5000)]
    public void Rent_should_return_buffer_with_at_least_requested_capacity()
    {
        var buf = TransportBuffer.Rent(1024);

        Assert.True(buf.Capacity >= 1024);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Rent_should_return_buffer_with_zero_length()
    {
        var buf = TransportBuffer.Rent(256);

        Assert.Equal(0, buf.Length);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Memory_should_reflect_length()
    {
        var buf = TransportBuffer.Rent(256);
        buf.Length = 42;

        Assert.Equal(42, buf.Memory.Length);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Span_should_reflect_length()
    {
        var buf = TransportBuffer.Rent(256);
        buf.Length = 10;

        Assert.Equal(10, buf.Span.Length);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void FullMemory_should_expose_entire_allocation()
    {
        var buf = TransportBuffer.Rent(256);
        buf.Length = 10;

        Assert.True(buf.FullMemory.Length >= 256);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Capacity_should_reflect_total_allocation()
    {
        var buf = TransportBuffer.Rent(512);

        Assert.True(buf.Capacity >= 512);
        Assert.Equal(buf.FullMemory.Length, buf.Capacity);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Dispose_should_return_to_pool()
    {
        var buf = TransportBuffer.Rent(64);
        buf.Dispose();

        var buf2 = TransportBuffer.Rent(64);

        Assert.Same(buf, buf2);

        buf2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Dispose_should_be_idempotent()
    {
        var buf = TransportBuffer.Rent(64);

        buf.Dispose();
        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void ConfigurePoolSize_should_control_max_pool_size()
    {
        var original = TransportBuffer.MaxPoolSize;
        try
        {
            TransportBuffer.ConfigurePoolSize(42);

            Assert.Equal(42, TransportBuffer.MaxPoolSize);
        }
        finally
        {
            TransportBuffer.ConfigurePoolSize(original);
        }
    }

    [Fact(Timeout = 5000)]
    public void Rent_should_reset_length_on_reused_buffer()
    {
        var buf = TransportBuffer.Rent(128);
        buf.Length = 100;
        buf.Dispose();

        var reused = TransportBuffer.Rent(128);

        Assert.Equal(0, reused.Length);

        reused.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Memory_should_be_writable()
    {
        var buf = TransportBuffer.Rent(64);
        buf.Length = 4;

        buf.Memory.Span[0] = 0xCA;
        buf.Memory.Span[1] = 0xFE;
        buf.Memory.Span[2] = 0xBA;
        buf.Memory.Span[3] = 0xBE;

        Assert.Equal(0xCA, buf.Span[0]);
        Assert.Equal(0xFE, buf.Span[1]);
        Assert.Equal(0xBA, buf.Span[2]);
        Assert.Equal(0xBE, buf.Span[3]);

        buf.Dispose();
    }
}
