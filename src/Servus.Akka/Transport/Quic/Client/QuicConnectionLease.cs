namespace Servus.Akka.Transport.Quic.Client;

internal sealed class QuicConnectionLease(QuicConnectionHandle handle, int maxConcurrentStreams) : IAsyncDisposable
{
    private readonly long _createdTicks = Environment.TickCount64;
    private bool _alive = true;

    public QuicConnectionHandle Handle { get; } = handle;

    public int ActiveStreams { get; private set; }

    public DateTime LastActivity { get; private set; } = DateTime.UtcNow;

    public bool IsAlive() => _alive;

    public bool IsExpired(TimeSpan maxLifetime)
    {
        if (maxLifetime == Timeout.InfiniteTimeSpan)
        {
            return false;
        }

        return Environment.TickCount64 - _createdTicks > (long)maxLifetime.TotalMilliseconds;
    }

    public bool CanAcceptStream() => _alive && ActiveStreams < maxConcurrentStreams;

    public void MarkBusy()
    {
        ActiveStreams++;
        LastActivity = DateTime.UtcNow;
    }

    public void MarkIdle()
    {
        ActiveStreams--;
        LastActivity = DateTime.UtcNow;
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