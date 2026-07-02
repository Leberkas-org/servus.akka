using System.Buffers;

namespace Servus.Akka.Transport;

/// <summary>
/// An <see cref="IMemoryOwner{T}"/> backed by an array rented from a cross-thread buffer pool,
/// returned to that pool exactly once on <see cref="Dispose"/>.
/// </summary>
/// <remarks>
/// The default <see cref="SharedPool"/> is a process-wide <see cref="ArrayPool{T}"/> built with
/// <see cref="ArrayPool{T}.Create(int,int)"/>, which uses global, locked per-bucket stacks (no
/// per-core affinity). A buffer rented on one thread and returned on another — an outbound wire
/// buffer freed after the socket flush, or an inbound body buffer rented on the connection-stage
/// thread and disposed on the application thread — is therefore reused instead of missing the pool.
/// <see cref="MemoryPool{T}.Shared"/> and the per-core <see cref="ArrayPool{T}.Shared"/> miss on that
/// thread hop and exhaust under HTTP/2-3 multiplexing (hundreds of concurrent in-flight buffers),
/// forcing fresh allocations. Routing both inbound and outbound buffers through this one pool was the
/// dominant client-side allocation win (~−79% managed bytes, ~+130% throughput on the Heavy upload).
/// </remarks>
public sealed class PooledArrayMemoryOwner : IMemoryOwner<byte>
{
    /// <summary>
    /// Process-wide cross-thread buffer pool shared by inbound body buffers and outbound wire buffers.
    /// Sized to the transport's real in-flight concurrency (an HTTP/2 connection multiplexing hundreds
    /// of streams, each with frames in flight).
    /// </summary>
    public static readonly ArrayPool<byte> SharedPool =
        ArrayPool<byte>.Create(maxArrayLength: 1024 * 1024, maxArraysPerBucket: 1024);

    private static readonly ObjectPool<PooledArrayMemoryOwner> WrapperPool = new(Environment.ProcessorCount * 8);

    private ArrayPool<byte> _pool = null!;
    private byte[]? _array;

    /// <summary>Rents from the shared cross-thread <see cref="SharedPool"/>.</summary>
    public static PooledArrayMemoryOwner Create(int minimumLength)
        => Create(SharedPool, minimumLength);

    /// <summary>Rents from a caller-supplied pool (e.g. a test or a dedicated per-subsystem pool).</summary>
    public static PooledArrayMemoryOwner Create(ArrayPool<byte> pool, int minimumLength)
    {
        if (!WrapperPool.TryRent(out var owner))
        {
            owner = new PooledArrayMemoryOwner();
        }

        owner._pool = pool;
        owner._array = pool.Rent(minimumLength);
        return owner;
    }

    public Memory<byte> Memory
        => _array ?? throw new ObjectDisposedException(nameof(PooledArrayMemoryOwner));

    public void Dispose()
    {
        var array = Interlocked.Exchange(ref _array, null);
        if (array is not null)
        {
            _pool.Return(array);
            WrapperPool.Return(this);
        }
    }
}
