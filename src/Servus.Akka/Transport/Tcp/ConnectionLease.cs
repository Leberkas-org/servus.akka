namespace Servus.Akka.Transport.Tcp;

internal sealed class ConnectionLease : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly ClientState _state;
    private readonly long _createdTicks = Environment.TickCount64;
    private bool _alive = true;

    internal ConnectionLease(ConnectionHandle handle, ClientState state, CancellationTokenSource cts, ConnectionInfo info)
    {
        Handle = handle;
        _state = state;
        _cts = cts;
        Info = info;
    }

    public ConnectionHandle Handle { get; }
    public ConnectionInfo Info { get; }

    internal ClientState State => _state;

    public bool IsAlive() => _alive;

    public bool IsExpired(TimeSpan maxLifetime)
    {
        if (maxLifetime == Timeout.InfiniteTimeSpan)
        {
            return false;
        }

        var elapsed = Environment.TickCount64 - _createdTicks;
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
        _state.Dispose();
    }
}
