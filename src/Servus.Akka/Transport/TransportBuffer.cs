using System.Buffers;
using System.Collections.Concurrent;

namespace Servus.Akka.Transport;

public sealed class TransportBuffer : IDisposable
{
    private static readonly ConcurrentStack<TransportBuffer> Pool = new();

    private static int _maxPoolSize = Environment.ProcessorCount * 4;

    private IMemoryOwner<byte>? _owner;

    public int Length { get; set; }

    public Memory<byte> Memory => _owner!.Memory[..Length];

    public ReadOnlySpan<byte> Span => _owner!.Memory.Span[..Length];

    public Memory<byte> FullMemory => _owner!.Memory;

    public int Capacity => _owner?.Memory.Length ?? 0;

    public static int MaxPoolSize => _maxPoolSize;

    public static void ConfigurePoolSize(int maxPoolSize)
    {
        _maxPoolSize = maxPoolSize;
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

        if (_maxPoolSize > 0 && Pool.Count < _maxPoolSize)
        {
            Pool.Push(this);
        }
    }
}
