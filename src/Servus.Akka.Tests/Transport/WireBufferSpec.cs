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

    private sealed class TrackingMemoryOwner(int size) : IMemoryOwner<byte>
    {
        private readonly byte[] _array = new byte[size];

        public bool Disposed { get; private set; }

        public Memory<byte> Memory => _array;

        public void Dispose() => Disposed = true;
    }
}
