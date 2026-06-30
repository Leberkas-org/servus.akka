using System.Buffers;

namespace Servus.Akka.Transport;

/// <summary>
/// A <see cref="MemoryPool{T}"/> facade over the cross-thread <see cref="PooledArrayMemoryOwner.SharedPool"/>,
/// for APIs that take a MemoryPool (System.IO.Pipelines). The default <see cref="MemoryPool{T}.Shared"/>
/// is the per-core ArrayPool, whose stacks exhaust under high-throughput transport buffering (the input
/// pipe segments for hundreds of concurrent multiplexed downloads, and the output pipe segments for
/// uploads) and miss when a segment is rented and returned on different threads. Backing the transport
/// pipes with the global, locked per-bucket cross-thread pool keeps those segments reused instead.
/// </summary>
public sealed class CrossThreadMemoryPool : MemoryPool<byte>
{
    public static readonly CrossThreadMemoryPool Instance = new();

    private CrossThreadMemoryPool()
    {
    }

    public override int MaxBufferSize => 1024 * 1024;

    public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
        => PooledArrayMemoryOwner.Create(minBufferSize <= 0 ? 4096 : minBufferSize);

    protected override void Dispose(bool disposing)
    {
        // The shared pool is process-wide and never disposed.
    }
}
