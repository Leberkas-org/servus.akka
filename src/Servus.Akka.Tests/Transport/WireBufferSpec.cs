using System.Buffers;
using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

[CollectionDefinition("WireBuffer", DisableParallelization = true)]
public class WireBufferCollection;

[Collection("WireBuffer")]
public sealed class WireBufferSpec
{
    [Fact(Timeout = 5000)]
    public void Rent_should_provide_writable_memory_of_at_least_requested_size()
    {
        var buf = WireBuffer.Rent(4 * 1024);

        Assert.True(buf.Capacity >= 4 * 1024);

        // Write/read roundtrip via FullMemory
        buf.FullMemory.Span[0] = 0xCA;
        buf.FullMemory.Span[1] = 0xFE;
        buf.Length = 2;

        Assert.Equal(0xCA, buf.Span[0]);
        Assert.Equal(0xFE, buf.Span[1]);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Wrap_array_with_offset_should_expose_sliced_memory()
    {
        var arr = new byte[20];
        arr[5] = 0xAB;
        arr[6] = 0xCD;

        var buf = WireBuffer.Wrap(arr, 5, 10);

        Assert.Equal(10, buf.Memory.Length);
        Assert.Equal(0xAB, buf.Span[0]);
        Assert.Equal(5, buf.Offset);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Dispose_should_be_idempotent_and_not_double_return_wrapper()
    {
        var buf = WireBuffer.Rent(64);

        buf.Dispose();
        buf.Dispose();

        var buf2 = WireBuffer.Rent(64);
        var buf3 = WireBuffer.Rent(64);

        Assert.False(ReferenceEquals(buf2, buf3));

        buf2.Dispose();
        buf3.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Wrap_owner_should_dispose_owner_on_dispose()
    {
        var owner = new TrackingMemoryOwner(32);
        var buf = WireBuffer.Wrap(owner, 0, 10);

        Assert.False(owner.Disposed);

        buf.Dispose();

        Assert.True(owner.Disposed);
    }

    [Fact(Timeout = 5000)]
    public void Rent_after_dispose_should_reuse_wrapper_with_reset_state()
    {
        var buf = WireBuffer.Rent(64);
        buf.Length = 42;
        buf.Dispose();

        var buf2 = WireBuffer.Rent(64);

        Assert.Equal(0, buf2.Length);
        Assert.Equal(0, buf2.Offset);

        buf2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Wrap_owner_beyond_logical_length_should_throw()
    {
        var outerArray = new byte[64];
        var owner = new SubsliceMemoryOwner(outerArray.AsMemory(8, 16));

        var ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => WireBuffer.Wrap(owner, 0, 32));

        Assert.Equal("length", ex.ParamName);
    }

    [Fact(Timeout = 5000)]
    public void Wrap_owner_with_nonzero_segment_offset_should_expose_correct_bytes()
    {
        var outerArray = new byte[64];
        for (int i = 0; i < outerArray.Length; i++)
        {
            outerArray[i] = (byte)(0x10 + i);
        }

        var owner = new SubsliceMemoryOwner(outerArray.AsMemory(8, 16));
        var buf = WireBuffer.Wrap(owner, 2, 4);

        Assert.Equal(4, buf.Memory.Length);
        Assert.Equal(outerArray[10], buf.Span[0]);
        Assert.Equal(outerArray[11], buf.Span[1]);
        Assert.Equal(outerArray[12], buf.Span[2]);
        Assert.Equal(outerArray[13], buf.Span[3]);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Rent_should_return_buffer_with_zero_length()
    {
        var buf = WireBuffer.Rent(256);

        Assert.Equal(0, buf.Length);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Memory_should_reflect_length()
    {
        var buf = WireBuffer.Rent(256);
        buf.Length = 42;

        Assert.Equal(42, buf.Memory.Length);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Span_should_reflect_length()
    {
        var buf = WireBuffer.Rent(256);
        buf.Length = 10;

        Assert.Equal(10, buf.Span.Length);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Capacity_should_reflect_total_allocation()
    {
        var buf = WireBuffer.Rent(512);

        Assert.True(buf.Capacity >= 512);
        Assert.Equal(buf.FullMemory.Length, buf.Capacity);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Dispose_should_return_to_pool()
    {
        var buf = WireBuffer.Rent(64);
        buf.Dispose();

        var buf2 = WireBuffer.Rent(64);

        Assert.Same(buf, buf2);

        buf2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Memory_should_be_writable()
    {
        var buf = WireBuffer.Rent(64);
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

    [Fact(Timeout = 5000)]
    public void Wrap_owner_should_bound_memory_and_span_by_length()
    {
        var buf = WireBuffer.Wrap(new TrackingMemoryOwner(64), 0, 10);

        Assert.Equal(10, buf.Length);
        Assert.Equal(10, buf.Memory.Length);
        Assert.Equal(10, buf.Span.Length);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Wrap_owner_should_expose_existing_data_without_copying()
    {
        var owner = new TrackingMemoryOwner(64);
        owner.Memory.Span[0] = 0xAB;
        owner.Memory.Span[1] = 0xCD;

        var buf = WireBuffer.Wrap(owner, 0, 2);

        Assert.Equal(0xAB, buf.Span[0]);
        Assert.Equal(0xCD, buf.Span[1]);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Wrap_owner_should_return_wrapper_to_pool_on_dispose()
    {
        var first = WireBuffer.Rent(64);
        first.Dispose();

        var buf = WireBuffer.Wrap(new TrackingMemoryOwner(16), 0, 4);

        Assert.Same(first, buf);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Wrap_owner_with_offset_should_expose_sliced_range()
    {
        var owner = new TrackingMemoryOwner(64);
        owner.Memory.Span[10] = 0x11;
        owner.Memory.Span[11] = 0x22;
        owner.Memory.Span[12] = 0x33;

        var buf = WireBuffer.Wrap(owner, 10, 3);

        Assert.Equal(3, buf.Length);
        Assert.Equal(3, buf.Memory.Length);
        Assert.Equal(3, buf.Span.Length);
        Assert.Equal(0x11, buf.Span[0]);
        Assert.Equal(0x22, buf.Span[1]);
        Assert.Equal(0x33, buf.Span[2]);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Wrap_owner_with_offset_should_not_leak_offset_into_reused_buffer()
    {
        var first = WireBuffer.Wrap(new TrackingMemoryOwner(64), 16, 8);
        first.Dispose();

        // The same instance is recycled; a plain Rent must start at offset 0 so Memory
        // covers [0..Length], not the previous sliced range.
        var reused = WireBuffer.Rent(64);
        Assert.Same(first, reused);
        reused.Length = 64;

        Assert.Equal(64, reused.Memory.Length);
        Assert.True(reused.FullMemory.Length >= 64);

        reused.Dispose();
    }

    private sealed class TrackingMemoryOwner(int size) : IMemoryOwner<byte>
    {
        private readonly byte[] _array = new byte[size];

        public bool Disposed { get; private set; }

        public Memory<byte> Memory => _array;

        public void Dispose() => Disposed = true;
    }

    private sealed class SubsliceMemoryOwner(Memory<byte> memory) : IMemoryOwner<byte>
    {
        public Memory<byte> Memory => memory;

        public void Dispose()
        {
        }
    }
}
