using System.Collections.Concurrent;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class ConnectionLeaseSpec
{
    private static ConnectionLease CreateLease()
    {
        var connection = new StreamConnection(Stream.Null, new TransportConnectionOptions());
        var cts = new CancellationTokenSource();
        var lease = new ConnectionLease(connection, cts, ConnectionInfo.None);
        return lease;
    }

    [Fact(Timeout = 5000)]
    public void ConnectionLease_should_expose_connection()
    {
        var lease = CreateLease();

        Assert.NotNull(lease.Connection);
    }

    [Fact(Timeout = 5000)]
    public void ConnectionLease_should_expose_options()
    {
        var lease = CreateLease();

        Assert.NotNull(lease.Options);
    }

    [Fact(Timeout = 5000)]
    public void ConnectionLease_should_expose_connection_instance()
    {
        var lease = CreateLease();

        Assert.NotNull(lease.Connection);
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

    // Reproduces the cross-actor double-dispose race (race-audit finding #2, M2): the consumer
    // state-machine actor and the pool-manager actor both call Dispose() on the SAME lease from
    // different threads. With a non-atomic `_alive` check-then-set both callers can pass the guard
    // and run the dispose body twice -> the CTS is cancelled/disposed twice (ObjectDisposedException
    // out of the second Cancel) AND the connection is disposed twice. The window is a couple of IL
    // instructions, so many racing leases are exercised to make the collision certain; the pass/fail
    // outcome is deterministic (an atomically-idempotent guard NEVER double-runs the body, a
    // non-atomic one eventually does).
    [Fact(Timeout = 30000)]
    public async Task Concurrent_dispose_from_two_paths_runs_the_body_exactly_once()
    {
        const int leaseCount = 1000;
        var threadsPerLease = Math.Max(4, Environment.ProcessorCount);
        var totalBodyRuns = 0;
        var failures = new ConcurrentQueue<Exception>();

        for (var i = 0; i < leaseCount; i++)
        {
            var connection = new CountingConnection();
            var cts = new CancellationTokenSource();
            var lease = new ConnectionLease(connection, cts, ConnectionInfo.None);

            using var barrier = new Barrier(threadsPerLease);
            var tasks = new Task[threadsPerLease];
            for (var t = 0; t < threadsPerLease; t++)
            {
                tasks[t] = Task.Run(() =>
                {
                    barrier.SignalAndWait();
                    try
                    {
                        lease.Dispose();
                    }
                    catch (Exception ex)
                    {
                        failures.Enqueue(ex);
                    }
                });
            }

            await Task.WhenAll(tasks);
            totalBodyRuns += connection.DisposeCount;
        }

        Assert.True(failures.IsEmpty,
            $"Dispose threw during a concurrent double-dispose: {failures.FirstOrDefault()}");
        Assert.Equal(leaseCount, totalBodyRuns);
    }

    private sealed class CountingConnection : IDuplexConnection
    {
        private int _disposeCount;

        public int DisposeCount => Volatile.Read(ref _disposeCount);

        public Action<int>? OnFlushed { get; set; }

        public ValueTask<WireBuffer?> ReceiveAsync() => throw new NotSupportedException();

        public bool TryEnqueue(WireBuffer buffer) => throw new NotSupportedException();

        public ValueTask<bool> QuiesceAsync() => throw new NotSupportedException();

        public Task CompleteAndDrainOutputAsync() => throw new NotSupportedException();

        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref _disposeCount);
            return ValueTask.CompletedTask;
        }
    }
}
