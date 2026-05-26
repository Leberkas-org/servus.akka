using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class ConnectionLeaseSpec
{
    private static ConnectionLease CreateLease()
    {
        var state = new ClientState(Stream.Null);
        var cts = new CancellationTokenSource();
        var handle = new ConnectionHandle(state.OutboundWriter, state.InboundReader, cts.Token);
        var lease = new ConnectionLease(handle, state, cts, ConnectionInfo.None);
        return lease;
    }

    [Fact(Timeout = 5000)]
    public void ConnectionLease_should_set_handle_from_constructor()
    {
        var lease = CreateLease();

        Assert.NotNull(lease.Handle);
    }

    [Fact(Timeout = 5000)]
    public void ConnectionLease_should_be_alive_when_created()
    {
        var lease = CreateLease();

        Assert.True(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public void ConnectionLease_should_set_is_alive_false_when_disposed()
    {
        var lease = CreateLease();

        lease.Dispose();

        Assert.False(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public void ConnectionLease_should_be_safe_when_disposed_twice()
    {
        var lease = CreateLease();

        lease.Dispose();
        lease.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void ConnectionLease_should_dispose_stream_when_disposed()
    {
        var memStream = new MemoryStream();
        var state = new ClientState(memStream);
        var cts = new CancellationTokenSource();
        var handle = new ConnectionHandle(state.OutboundWriter, state.InboundReader, cts.Token);
        var lease = new ConnectionLease(handle, state, cts, ConnectionInfo.None);

        lease.Dispose();

        Assert.Throws<ObjectDisposedException>(() => memStream.ReadByte());
    }

    [Fact(Timeout = 5000)]
    public void IsExpired_should_return_false_for_infinite_lifetime()
    {
        var lease = CreateLease();

        Assert.False(lease.IsExpired(Timeout.InfiniteTimeSpan));
    }

    [Fact(Timeout = 5000)]
    public void IsExpired_should_return_false_for_recent_connection()
    {
        var lease = CreateLease();

        Assert.False(lease.IsExpired(TimeSpan.FromMinutes(1)));
    }

    [Fact(Timeout = 5000)]
    public async Task IsExpired_should_return_true_for_very_short_lifetime()
    {
        var lease = CreateLease();

        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.True(lease.IsExpired(TimeSpan.FromMilliseconds(1)));
    }

    [Fact(Timeout = 5000)]
    public void IsExpired_should_treat_minus_one_ms_as_infinite()
    {
        var lease = CreateLease();

        Assert.False(lease.IsExpired(TimeSpan.FromMilliseconds(-1)));
    }

    [Fact(Timeout = 5000)]
    public async Task IsExpired_should_consider_zero_timespan_as_expired_after_tick()
    {
        var lease = CreateLease();

        await Task.Delay(2, TestContext.Current.CancellationToken);
        Assert.True(lease.IsExpired(TimeSpan.Zero));
    }

    [Fact(Timeout = 5000)]
    public void Idempotent_double_dispose_should_not_throw()
    {
        var lease = CreateLease();

        lease.Dispose();
        lease.Dispose();

        Assert.False(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public void Handle_should_reflect_cancelled_state_after_dispose()
    {
        var lease = CreateLease();

        Assert.False(lease.Handle.IsCancelled);

        lease.Dispose();

        Assert.True(lease.Handle.IsCancelled);
    }
}