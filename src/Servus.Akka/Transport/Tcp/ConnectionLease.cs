namespace Servus.Akka.Transport.Tcp;

internal sealed class ConnectionLease : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TimeProvider _clock;
    private readonly long _createdTicks;
    // 1 = alive, 0 = disposed. Read/written with Interlocked so the idempotency guard is atomic:
    // the lease is disposed from two different actors (consumer state machine + pool manager) on
    // different threads, so a plain check-then-set bool would let both pass the guard and run the
    // dispose body twice (double CTS cancel/dispose -> ObjectDisposedException, double connection
    // dispose). Interlocked is appropriate here — a genuine cross-actor/cross-thread boundary.
    private int _alive = 1;
    private long _idleSinceTicks = -1;

    internal ConnectionLease(
        IDuplexConnection connection,
        CancellationTokenSource cts,
        ConnectionInfo info,
        TimeProvider? timeProvider = null,
        TransportConnectionOptions? options = null)
    {
        Connection = connection;
        _cts = cts;
        Info = info;
        Options = options ?? new TransportConnectionOptions();
        _clock = timeProvider ?? TimeProvider.System;
        _createdTicks = _clock.GetUtcNow().ToUnixTimeMilliseconds();
    }

    public IDuplexConnection Connection { get; }
    public ConnectionInfo Info { get; }

    /// <summary>Watermarks / receive hint the transport was constructed with; consumed by the client
    /// state machine for outbound backpressure. Carried on the lease to keep the connection interface
    /// minimal.</summary>
    public TransportConnectionOptions Options { get; }

    public bool IsAlive() => Volatile.Read(ref _alive) == 1;

    public bool IsExpired(TimeSpan maxLifetime)
    {
        if (maxLifetime == Timeout.InfiniteTimeSpan)
        {
            return false;
        }

        var elapsed = _clock.GetUtcNow().ToUnixTimeMilliseconds() - _createdTicks;
        var lifetimeMs = (long)maxLifetime.TotalMilliseconds;
        return lifetimeMs <= 0 || elapsed > lifetimeMs;
    }

    /// <summary>Records the moment this lease entered a pool's idle set. Call when the lease is
    /// pushed to the idle collection; the timestamp seeds <see cref="IsIdleExpired"/>.</summary>
    public void MarkIdle()
    {
        _idleSinceTicks = _clock.GetUtcNow().ToUnixTimeMilliseconds();
    }

    public bool IsIdleExpired(TimeSpan idleTimeout)
    {
        if (idleTimeout == Timeout.InfiniteTimeSpan || _idleSinceTicks < 0)
        {
            return false;
        }

        var elapsed = _clock.GetUtcNow().ToUnixTimeMilliseconds() - _idleSinceTicks;
        var idleMs = (long)idleTimeout.TotalMilliseconds;
        return idleMs <= 0 || elapsed > idleMs;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _alive, 0) == 0)
        {
            return;
        }

        _cts.Cancel();
        _cts.Dispose();
        _ = Connection.DisposeAsync();
    }
}
