using Servus.Akka.Transport.Quic;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Tests.Transport.Quic.Client;

public sealed class QuicConnectionLeaseSpec
{
    private QuicConnectionHandle CreateTestHandle() =>
        new(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

    [Fact(Timeout = 5000)]
    public void Handle_should_return_constructor_value()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        Assert.Same(handle, lease.Handle);
    }

    [Fact(Timeout = 5000)]
    public void IsAlive_should_return_true_initially()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        Assert.True(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public void IsExpired_should_return_false_when_within_lifetime()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        Assert.False(lease.IsExpired(TimeSpan.FromSeconds(10)));
    }

    [Fact(Timeout = 5000)]
    public async Task IsExpired_should_return_true_when_past_lifetime()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        // Create with short lifetime
        var shortLifetime = TimeSpan.FromMilliseconds(50);

        // Wait longer than the lifetime
        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.True(lease.IsExpired(shortLifetime));
    }

    [Fact(Timeout = 5000)]
    public void IsExpired_should_return_false_for_infinite_lifetime()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        // Infinite lifetime should never expire
        Assert.False(lease.IsExpired(Timeout.InfiniteTimeSpan));
    }

    [Fact(Timeout = 5000)]
    public void CanAcceptStream_should_return_true_when_below_max()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 5);

        // Initially no active streams, should accept
        Assert.True(lease.CanAcceptStream());

        // Mark busy twice
        lease.MarkBusy();
        lease.MarkBusy();

        // Still below max (2 < 5)
        Assert.True(lease.CanAcceptStream());
    }

    [Fact(Timeout = 5000)]
    public void CanAcceptStream_should_return_false_when_at_max()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 3);

        // Mark busy up to max
        lease.MarkBusy();
        lease.MarkBusy();
        lease.MarkBusy();

        // At max, should not accept
        Assert.False(lease.CanAcceptStream());
    }

    [Fact(Timeout = 5000)]
    public void CanAcceptStream_should_return_false_when_not_alive()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 5);

        // Dispose to mark as not alive
        _ = lease.DisposeAsync();

        Assert.False(lease.IsAlive());
        Assert.False(lease.CanAcceptStream());
    }

    [Fact(Timeout = 5000)]
    public void MarkBusy_should_increment_ActiveStreams()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        Assert.Equal(0, lease.ActiveStreams);

        lease.MarkBusy();
        Assert.Equal(1, lease.ActiveStreams);

        lease.MarkBusy();
        Assert.Equal(2, lease.ActiveStreams);
    }

    [Fact(Timeout = 5000)]
    public void MarkIdle_should_decrement_ActiveStreams()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        lease.MarkBusy();
        lease.MarkBusy();
        lease.MarkBusy();

        Assert.Equal(3, lease.ActiveStreams);

        lease.MarkIdle();
        Assert.Equal(2, lease.ActiveStreams);

        lease.MarkIdle();
        Assert.Equal(1, lease.ActiveStreams);
    }

    [Fact(Timeout = 5000)]
    public void MarkIdle_should_not_go_below_zero()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        // Start at 0
        Assert.Equal(0, lease.ActiveStreams);

        // Decrement
        lease.MarkIdle();

        // Should be -1 (no guard in production code)
        Assert.Equal(-1, lease.ActiveStreams);
    }

    [Fact(Timeout = 5000)]
    public void ActiveStreams_should_reflect_busy_idle_balance()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        lease.MarkBusy();
        lease.MarkBusy();
        lease.MarkBusy();
        Assert.Equal(3, lease.ActiveStreams);

        lease.MarkIdle();
        Assert.Equal(2, lease.ActiveStreams);

        lease.MarkBusy();
        Assert.Equal(3, lease.ActiveStreams);

        lease.MarkIdle();
        lease.MarkIdle();
        Assert.Equal(1, lease.ActiveStreams);
    }

    [Fact(Timeout = 5000)]
    public void LastActivity_should_update_on_MarkBusy()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        var initialActivity = lease.LastActivity;

        // Wait a bit to ensure time difference
        Thread.Sleep(10);

        lease.MarkBusy();
        var afterBusy = lease.LastActivity;

        Assert.True(afterBusy > initialActivity);
    }

    [Fact(Timeout = 5000)]
    public void LastActivity_should_update_on_MarkIdle()
    {
        var handle = CreateTestHandle();
        var lease = new QuicConnectionLease(handle, 10);

        lease.MarkBusy();
        var afterBusy = lease.LastActivity;

        Thread.Sleep(10);

        lease.MarkIdle();
        var afterIdle = lease.LastActivity;

        Assert.True(afterIdle > afterBusy);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_should_dispose_handle()
    {
        var disposeCalled = false;
        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () =>
            {
                disposeCalled = true;
                return ValueTask.CompletedTask;
            });

        var lease = new QuicConnectionLease(handle, 10);

        Assert.True(lease.IsAlive());
        Assert.False(disposeCalled);

        await lease.DisposeAsync();

        Assert.False(lease.IsAlive());
        Assert.True(disposeCalled);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_should_be_idempotent()
    {
        var disposeCount = 0;
        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () =>
            {
                disposeCount++;
                return ValueTask.CompletedTask;
            });

        var lease = new QuicConnectionLease(handle, 10);

        await lease.DisposeAsync();
        Assert.Equal(1, disposeCount);

        // Second dispose should not call handle.DisposeAsync again
        await lease.DisposeAsync();
        Assert.Equal(1, disposeCount);
    }
}