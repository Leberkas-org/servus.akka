namespace Servus.Akka.Transport.Quic.Client;

internal sealed class QuicConnectionLease : IAsyncDisposable
{
    private readonly TimeProvider _clock;
    private readonly long _createdTicks;
    private readonly int _maxConcurrentStreams;
    private readonly TimeSpan _idleTimeout;

    // 1 = alive, 0 = disposed. Read/written with Interlocked so the idempotency guard is atomic: the
    // lease is disposed from two different actors (consumer transport state machine + pool manager)
    // on different threads, so a plain check-then-set bool would let both pass the guard and dispose
    // the underlying handle twice. Interlocked is appropriate here — a genuine cross-actor boundary.
    private int _alive = 1;

    public QuicConnectionLease(QuicConnectionHandle handle, int maxConcurrentStreams, TimeProvider? timeProvider = null,
        TimeSpan idleTimeout = default)
    {
        Handle = handle;
        _maxConcurrentStreams = maxConcurrentStreams;
        _idleTimeout = idleTimeout;
        _clock = timeProvider ?? TimeProvider.System;
        _createdTicks = _clock.GetUtcNow().ToUnixTimeMilliseconds();
        LastActivity = _clock.GetUtcNow().UtcDateTime;
    }

    public QuicConnectionHandle Handle { get; }

    public int ActiveStreams { get; private set; }

    public DateTime LastActivity { get; private set; }

    public bool IsAlive() => Volatile.Read(ref _alive) == 1;

    public bool IsExpired(TimeSpan maxLifetime)
    {
        if (maxLifetime == Timeout.InfiniteTimeSpan)
        {
            return false;
        }

        return _clock.GetUtcNow().ToUnixTimeMilliseconds() - _createdTicks > (long)maxLifetime.TotalMilliseconds;
    }

    public bool CanAcceptStream() => Volatile.Read(ref _alive) == 1 && !IsIdleClosed() && ActiveStreams < _maxConcurrentStreams;

    // Heuristic: MsQuic silently closes a connection after its negotiated idle timeout, but the lease is
    // not notified — _alive stays true and the pool would hand out a dead connection (the next
    // OpenOutboundStreamAsync throws QuicException(ConnectionIdle)). Treat a connection with no stream
    // activity for at least the idle timeout as closed, so the pool establishes a fresh one instead. Only
    // applies while idle (ActiveStreams == 0); an in-use connection is never considered idle-closed.
    private bool IsIdleClosed()
    {
        if (_idleTimeout <= TimeSpan.Zero || _idleTimeout == Timeout.InfiniteTimeSpan || ActiveStreams > 0)
        {
            return false;
        }

        return _clock.GetUtcNow().UtcDateTime - LastActivity >= _idleTimeout;
    }

    public void MarkBusy()
    {
        ActiveStreams++;
        LastActivity = _clock.GetUtcNow().UtcDateTime;
    }

    public void MarkIdle()
    {
        ActiveStreams--;
        LastActivity = _clock.GetUtcNow().UtcDateTime;
    }


    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _alive, 0) == 0)
        {
            return;
        }

        await Handle.DisposeAsync().ConfigureAwait(false);
    }
}