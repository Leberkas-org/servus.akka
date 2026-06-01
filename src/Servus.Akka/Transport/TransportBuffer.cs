using System.Buffers;
using System.Collections.Concurrent;

namespace Servus.Akka.Transport;

public sealed class TransportBuffer : IDisposable
{
    private static readonly ConcurrentStack<TransportBuffer> Pool = new();

    private IMemoryOwner<byte>? _owner;

    public int Length { get; set; }

    public Memory<byte> Memory => _owner!.Memory[..Length];

    public ReadOnlySpan<byte> Span => _owner!.Memory.Span[..Length];

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
        if (!Pool.TryPop(out var buf))
        {
            return new TransportBuffer { _owner = owner };
        }

        buf._owner = owner;
        buf.Length = 0;
        return buf;
    }

    // Wraps an existing IMemoryOwner without renting/copying. The returned buffer takes
    // ownership of 'owner' and disposes it on Dispose — use when the data already lives in a
    // pooled buffer that can be handed off directly (e.g. an outbound body chunk).
    public static TransportBuffer Wrap(IMemoryOwner<byte> owner, int length)
    {
        if (!Pool.TryPop(out var buf))
        {
            return new TransportBuffer { _owner = owner, Length = length };
        }

        buf._owner = owner;
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

        if (MaxPoolSize > 0 && Pool.Count < MaxPoolSize)
        {
            Pool.Push(this);
        }
    }
}
