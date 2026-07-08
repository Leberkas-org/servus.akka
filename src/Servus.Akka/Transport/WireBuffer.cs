using System.Buffers;

namespace Servus.Akka.Transport;

/// <summary>
/// The transport's single buffer owner: an array from the cross-thread <see cref="SharedPool"/>
/// (or a wrapped external array/owner) plus offset/length. Ownership transfers with the instance —
/// whoever holds it disposes it exactly once; Dispose returns the array to its pool.
/// <para>
/// The wrapper object itself is NOT pooled: each rent allocates a fresh instance. Pooling a ~40-byte
/// wrapper only trades a Gen0 bump-allocation for a contended object-pool rent/return, which measures
/// break-even-or-worse under HTTP/2-3 cross-thread multiplexing — and a fresh instance per rent makes
/// the spent flag permanently reliable, so a stale post-re-rent dispose is structurally impossible
/// (no ABA). The expensive pooling — the <see cref="SharedPool"/> array pool — is retained.
/// </para>
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

    private byte[]? _array;
    private ArrayPool<byte>? _returnPool;      // null: array is not pool-owned (external Wrap)
    private IDisposable? _externalOwner;       // set only by Wrap(IMemoryOwner, ...)
    private int _offset;
    private bool _disposed;

    public int Length { get; set; }
    public int Offset => _offset;
    public int Capacity => _array?.Length ?? 0;
    public Memory<byte> Memory { get { ThrowIfDisposed(); return _array.AsMemory(_offset, Length); } }
    public ReadOnlySpan<byte> Span { get { ThrowIfDisposed(); return _array.AsSpan(_offset, Length); } }
    public Memory<byte> FullMemory { get { ThrowIfDisposed(); return _array.AsMemory(); } }

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

    private static WireBuffer RentWrapper() => new();

    public void Dispose()
    {
        // Idempotent: a second Dispose is a safe no-op. Because the wrapper is never reused across
        // owners, the spent flag stays true for the life of this instance — so a stale, post-re-rent
        // dispose from another owner cannot free or corrupt anyone else's array.
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        var array = _array;
        _array = null;
        _returnPool?.Return(array!);
        _returnPool = null;
        _externalOwner?.Dispose();
        _externalOwner = null;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(WireBuffer),
                "This WireBuffer was disposed; its backing array was returned to the pool. " +
                "Accessing Memory/Span/FullMemory after disposal is a use-after-free.");
        }
    }
}
