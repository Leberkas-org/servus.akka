namespace Servus.Akka.Transport.Tcp;

internal sealed class ConnectionLease : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TimeProvider _clock;
    private readonly long _createdTicks;
    private bool _alive = true;

    internal ConnectionLease(ConnectionHandle handle, ClientState state, CancellationTokenSource cts, ConnectionInfo info,
        TimeProvider? timeProvider = null)
    {
        Handle = handle;
        State = state;
        _cts = cts;
        Info = info;
        _clock = timeProvider ?? TimeProvider.System;
        _createdTicks = _clock.GetUtcNow().ToUnixTimeMilliseconds();
    }

    public ConnectionHandle Handle { get; }
    public ConnectionInfo Info { get; }

    internal ClientState State { get; }

    public bool IsAlive() => _alive;

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

    public void Dispose()
    {
        if (!_alive)
        {
            return;
        }

        _alive = false;
        _cts.Cancel();
        _cts.Dispose();
        State.Dispose();
    }
}
