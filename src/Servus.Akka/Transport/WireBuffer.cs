using System.Buffers;
using static Servus.Senf;

namespace Servus.Akka.Transport;

/// <summary>
/// The transport's single pooled buffer owner: an array from the cross-thread <see cref="SharedPool"/>
/// (or a wrapped external array/owner) plus offset/length, in one pooled wrapper. Ownership transfers
/// with the instance — whoever holds it disposes it exactly once; Dispose returns the array to its
/// pool and the wrapper to the wrapper pool.
/// </summary>
public sealed class WireBuffer : IMemoryOwner<byte>
{
    /// <summary>
    /// Process-wide cross-thread buffer pool (locked per-bucket stacks, no per-core affinity), so a
    /// buffer rented on one thread and returned on another is reused instead of missing the pool.
    /// Per-core ArrayPool&lt;byte&gt;.Shared / MemoryPool.Shared miss on that hop and exhaust under
    /// HTTP/2-3 multiplexing.
    /// </summary>
    public static readonly ArrayPool<byte> SharedPool =
        ArrayPool<byte>.Create(maxArrayLength: 1024 * 1024, maxArraysPerBucket: 1024);

    private static ObjectPool<WireBuffer> _wrapperPool = new(1024);

    /// <summary>Startup-only: replaces the wrapper pool. Not safe once buffers are in flight.</summary>
    public static void ConfigureWrapperPool(int size) => _wrapperPool = new ObjectPool<WireBuffer>(size);

    private byte[]? _array;
    private ArrayPool<byte>? _returnPool;      // null: array is not pool-owned (external Wrap)
    private IDisposable? _externalOwner;       // set only by Wrap(IMemoryOwner, ...)
    private int _offset;

    public int Length { get; set; }
    public int Offset => _offset;
    public int Capacity => _array?.Length ?? 0;
    public Memory<byte> Memory => _array.AsMemory(_offset, Length);
    public ReadOnlySpan<byte> Span => _array.AsSpan(_offset, Length);
    public Memory<byte> FullMemory => _array.AsMemory();

    public static WireBuffer Rent(int minimumSize)
    {
        var buf = RentWrapper();
        buf._array = SharedPool.Rent(minimumSize);
        buf._returnPool = SharedPool;
        return buf;
    }

    public static WireBuffer Wrap(byte[] array, int offset, int length, ArrayPool<byte>? returnPool = null)
    {
        var buf = RentWrapper();
        buf._array = array;
        buf._returnPool = returnPool;
        buf._offset = offset;
        buf.Length = length;
        return buf;
    }

    // Migration bridge: wraps an external IMemoryOwner whose memory is array-backed. The buffer
    // owns 'owner' and disposes it on Dispose. Transport hot paths use the array overloads; this
    // exists for consumers (GaudiHTTP) whose data already lives in a foreign owner.
    public static WireBuffer Wrap(IMemoryOwner<byte> owner, int offset, int length)
    {
        if (!System.Runtime.InteropServices.MemoryMarshal.TryGetArray<byte>(owner.Memory, out var seg))
        {
            throw new ArgumentException("WireBuffer.Wrap requires an array-backed owner.", nameof(owner));
        }

        if (offset < 0 || length < 0 || offset + length > seg.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(length),
                "Wrapped range exceeds the owner's memory.");
        }

        var buf = Wrap(seg.Array!, seg.Offset + offset, length);
        buf._externalOwner = owner;
        return buf;
    }

    private static WireBuffer RentWrapper()
    {
        if (!_wrapperPool.TryRent(out var buf))
        {
            buf = new WireBuffer();
        }

        return buf;
    }

    public void Dispose()
    {
        var array = Interlocked.Exchange(ref _array, null);
        if (array is null)
        {
            // Double-dispose: the first Dispose already returned this wrapper to the pool. Returning
            // it AGAIN would hand the same instance to two renters (buffer aliasing / silent
            // cross-connection corruption). Log the culprit and bail.
            Tracing.For("Transport").Warning(this,
                "WireBuffer double-dispose detected — wrapper NOT re-returned to pool. Stack: {0}",
                Environment.StackTrace);
            return;
        }

        _returnPool?.Return(array);
        _returnPool = null;
        _externalOwner?.Dispose();
        _externalOwner = null;
        _offset = 0;
        Length = 0;
        _wrapperPool.Return(this);
    }
}
