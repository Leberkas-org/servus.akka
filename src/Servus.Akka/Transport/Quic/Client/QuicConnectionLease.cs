namespace Servus.Akka.Transport.Quic.Client;

internal sealed class QuicConnectionLease : IAsyncDisposable
{
    private readonly TimeProvider _clock;
    private readonly long _createdTicks;
    private readonly int _maxConcurrentStreams;
    private bool _alive = true;

    public QuicConnectionLease(QuicConnectionHandle handle, int maxConcurrentStreams, TimeProvider? timeProvider = null)
    {
        Handle = handle;
        _maxConcurrentStreams = maxConcurrentStreams;
        _clock = timeProvider ?? TimeProvider.System;
        _createdTicks = _clock.GetUtcNow().ToUnixTimeMilliseconds();
        LastActivity = _clock.GetUtcNow().UtcDateTime;
    }

    public QuicConnectionHandle Handle { get; }

    public int ActiveStreams { get; private set; }

    public DateTime LastActivity { get; private set; }

    public bool IsAlive() => _alive;

    public bool IsExpired(TimeSpan maxLifetime)
    {
        if (maxLifetime == Timeout.InfiniteTimeSpan)
        {
            return false;
        }

        return _clock.GetUtcNow().ToUnixTimeMilliseconds() - _createdTicks > (long)maxLifetime.TotalMilliseconds;
    }

    public bool CanAcceptStream() => _alive && ActiveStreams < _maxConcurrentStreams;

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
        if (!_alive)
        {
            return;
        }

        _alive = false;
        await Handle.DisposeAsync().ConfigureAwait(false);
    }
}