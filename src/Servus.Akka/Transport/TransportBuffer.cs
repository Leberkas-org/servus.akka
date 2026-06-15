using System.Buffers;

namespace Servus.Akka.Transport;

public sealed class TransportBuffer : IDisposable
{
    private static readonly ObjectPool<TransportBuffer> Pool = new(Environment.ProcessorCount * 4);

    private IMemoryOwner<byte>? _owner;
    private int _offset;

    public int Length { get; set; }

    public Memory<byte> Memory => _owner!.Memory.Slice(_offset, Length);

    public ReadOnlySpan<byte> Span => _owner!.Memory.Span.Slice(_offset, Length);

    public Memory<byte> FullMemory => _owner!.Memory;

    public int Capacity => _owner?.Memory.Length ?? 0;

    public static int MaxPoolSize { get; private set; } = Environment.ProcessorCount * 4;

    public static void ConfigurePoolSize(int maxPoolSize)
    {
        MaxPoolSize = maxPoolSize;
    }

    public static TransportBuffer Rent(int minimumSize)
    {
        var owner = MemoryPool<byte>.Shared.Rent(minimumSize);
        if (!Pool.TryRent(out var buf))
        {
            return new TransportBuffer { _owner = owner };
        }

        buf._owner = owner;
        buf._offset = 0;
        buf.Length = 0;
        return buf;
    }

    // Wraps an existing IMemoryOwner without renting/copying. The returned buffer takes
    // ownership of 'owner' and disposes it on Dispose — use when the data already lives in a
    // pooled buffer that can be handed off directly (e.g. an outbound body chunk).
    public static TransportBuffer Wrap(IMemoryOwner<byte> owner, int length)
    {
        return Wrap(owner, 0, length);
    }

    // Wraps an existing IMemoryOwner with an offset, exposing _owner.Memory[offset..offset+length].
    // Use for zero-copy frame emission when the frame header was written into headroom at 'offset'
    // and the payload immediately follows it. The offset is stored on the buffer itself and applied
    // by the Memory/Span getters — no extra IMemoryOwner wrapper is allocated. The buffer still owns
    // 'owner' and disposes it on Dispose, exactly as the non-offset overload does.
    public static TransportBuffer Wrap(IMemoryOwner<byte> owner, int offset, int length)
    {
        if (!Pool.TryRent(out var buf))
        {
            return new TransportBuffer { _owner = owner, _offset = offset, Length = length };
        }

        buf._owner = owner;
        buf._offset = offset;
        buf.Length = length;
        return buf;
    }

    public static implicit operator TransportBuffer(byte[] data)
    {
        var buf = Rent(data.Length);
        data.AsSpan().CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        return buf;
    }

    public void Dispose()
    {
        var owner = Interlocked.Exchange(ref _owner, null);
        owner?.Dispose();
        _offset = 0;

        if (MaxPoolSize > 0)
        {
            Pool.Return(this);
        }
    }
}
