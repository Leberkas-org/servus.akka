using Akka.Actor;
using Akka.TestKit.Xunit;
using Microsoft.Extensions.Time.Testing;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Tests.Transport.Quic.Client;

public sealed class QuicConnectionManagerActorSpec : TestKit
{
    private static QuicTransportOptions CreateOptions() => new()
    {
        Host = "localhost",
        Port = 443
    };

    private static IQuicConnectionFactory CreateMockFactory(bool shouldFail = false, int maxStreams = 100)
    {
        return new MockFactory(shouldFail, maxStreams);
    }

    private IActorRef CreateActor(IQuicConnectionFactory? factory = null)
    {
        var f = factory ?? CreateMockFactory();
        return Sys.ActorOf(TransportFactory.CreateQuicConnectionManager(f));
    }

    [Fact(Timeout = 5000)]
    public async Task AcquireAsync_should_return_lease()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.NotNull(lease);
        Assert.True(lease.IsAlive());

        await lease.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task AcquireAsync_should_call_factory_EstablishAsync()
    {
        var factory = new MockFactory();
        var actor = CreateActor(factory);
        var options = CreateOptions();

        Assert.Equal(0, factory.EstablishCount);

        var lease = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.Equal(1, factory.EstablishCount);

        await lease.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task AcquireAsync_should_fail_when_factory_throws()
    {
        var factory = new MockFactory(shouldFail: true);
        var actor = CreateActor(factory);
        var options = CreateOptions();

        await Assert.ThrowsAnyAsync<IOException>(() =>
            QuicConnectionManagerActor.AcquireAsync(actor, options, TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = 5000)]
    public async Task Release_with_CanReuse_true_should_not_dispose()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        actor.Tell(new QuicConnectionManagerActor.Release(lease, CanReuse: true));

        Assert.True(lease.IsAlive());

        await lease.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Release_with_CanReuse_false_should_dispose()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        actor.Tell(new QuicConnectionManagerActor.Release(lease, CanReuse: false));

        AwaitCondition(() => !lease.IsAlive(), TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken);

        Assert.False(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public async Task Multiple_acquires_should_create_multiple_connections()
    {
        var factory = new MockFactory();
        var actor = CreateActor(factory);
        var options = CreateOptions() with
        {
            MaxBidirectionalStreams = 1,
            MaxConnectionsPerHost = 2
        };

        var lease1 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        var lease2 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.NotSame(lease1, lease2);
        Assert.Equal(2, factory.EstablishCount);

        await lease1.DisposeAsync();
        await lease2.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Acquire_should_respect_cancellation()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            QuicConnectionManagerActor.AcquireAsync(actor, options, cts.Token));
    }

    [Fact(Timeout = 5000)]
    public async Task OnEvict_should_remove_idle_dead_leases()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease1 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await lease1.DisposeAsync();

        actor.Tell(new QuicConnectionManagerActor.Release(lease1, CanReuse: true));

        actor.Tell(QuicConnectionManagerActor.Evict.Instance);

        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.False(lease1.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public async Task OnEvict_should_not_remove_active_leases()
    {
        var actor = CreateActor();
        var options = CreateOptions() with { ConnectionLifetime = TimeSpan.FromMilliseconds(50) };

        var lease = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await Task.Delay(100, TestContext.Current.CancellationToken);

        actor.Tell(QuicConnectionManagerActor.Evict.Instance);

        Assert.True(lease.IsAlive());

        await lease.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task OnEstablished_should_mark_lease_busy()
    {
        var factory = new MockFactory();
        var actor = CreateActor(factory);
        var options = CreateOptions();

        var lease = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.NotNull(lease);
        Assert.True(lease.IsAlive());
        Assert.Equal(1, lease.ActiveStreams);

        await lease.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task OnRelease_should_not_dispose_when_can_reuse_and_alive()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        actor.Tell(new QuicConnectionManagerActor.Release(lease, CanReuse: true));

        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.True(lease.IsAlive());

        await lease.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task OnRelease_should_dispose_when_not_alive()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await lease.DisposeAsync();

        actor.Tell(new QuicConnectionManagerActor.Release(lease, CanReuse: true));

        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.False(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public async Task Multiple_hosts_should_maintain_separate_pools()
    {
        var factory = new MockFactory();
        var actor = CreateActor(factory);
        var options1 = CreateOptions() with { Host = "host1.example.com" };
        var options2 = CreateOptions() with { Host = "host2.example.com" };

        var lease1 = await QuicConnectionManagerActor.AcquireAsync(actor, options1,
            TestContext.Current.CancellationToken);
        var lease2 = await QuicConnectionManagerActor.AcquireAsync(actor, options2,
            TestContext.Current.CancellationToken);

        Assert.NotSame(lease1, lease2);
        Assert.Equal(2, factory.EstablishCount);

        await lease1.DisposeAsync();
        await lease2.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Acquire_should_queue_when_max_connections_reached()
    {
        var slowFactory = new SlowQuicConnectionFactory(TimeSpan.FromSeconds(1));
        var actor = CreateActor(slowFactory);
        var options = CreateOptions() with { MaxConnectionsPerHost = 1 };

        var acquire1Task = QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await QuicConnectionManagerActor.AcquireAsync(actor, options, cts.Token);
        });

        var lease1 = await acquire1Task;
        await lease1.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Acquire_should_reuse_idle_lease_when_available()
    {
        var factory = new MockFactory();
        var actor = CreateActor(factory);
        var options = CreateOptions();

        var lease1 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new QuicConnectionManagerActor.Release(lease1, CanReuse: true));

        var lease2 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.Same(lease1, lease2);
        Assert.Equal(1, factory.EstablishCount);

        await lease2.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Evict_should_remove_idle_lease_past_lifetime_with_injected_clock()
    {
        var clock = new FakeTimeProvider();
        var factory = new MockFactory(timeProvider: clock);
        var actor = CreateActor(factory);
        var options = CreateOptions();

        var lease1 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        // Release so the lease is idle (ActiveStreams == 0), making it eligible for eviction.
        actor.Tell(new QuicConnectionManagerActor.Release(lease1, CanReuse: true));

        // Age past the manager's idle-eviction window, then run the sweep.
        clock.Advance(TimeSpan.FromMinutes(11));
        actor.Tell(QuicConnectionManagerActor.Evict.Instance);

        var lease2 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.NotSame(lease1, lease2);
        Assert.Equal(2, factory.EstablishCount);

        await lease2.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Evict_should_remove_idle_lease_past_a_short_configured_lifetime()
    {
        var clock = new FakeTimeProvider();
        var factory = new MockFactory(timeProvider: clock);
        var actor = CreateActor(factory);
        var options = CreateOptions() with { ConnectionLifetime = TimeSpan.FromMilliseconds(50) };

        var lease1 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new QuicConnectionManagerActor.Release(lease1, CanReuse: true));

        clock.Advance(TimeSpan.FromMilliseconds(200));
        actor.Tell(QuicConnectionManagerActor.Evict.Instance);

        var lease2 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.NotSame(lease1, lease2);
        Assert.Equal(2, factory.EstablishCount);

        await lease2.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Evict_should_never_remove_an_idle_lease_with_infinite_lifetime()
    {
        var clock = new FakeTimeProvider();
        var factory = new MockFactory(timeProvider: clock);
        var actor = CreateActor(factory);
        var options = CreateOptions() with { ConnectionLifetime = Timeout.InfiniteTimeSpan };

        var lease1 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new QuicConnectionManagerActor.Release(lease1, CanReuse: true));

        // Advance well past the old hardcoded 10-minute eviction window.
        clock.Advance(TimeSpan.FromMinutes(30));
        actor.Tell(QuicConnectionManagerActor.Evict.Instance);

        var lease2 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.Same(lease1, lease2);
        Assert.Equal(1, factory.EstablishCount);

        await lease2.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Release_should_dispatch_pending_acquire_when_stream_becomes_available()
    {
        var factory = new MockFactory(maxStreams: 1);
        var actor = CreateActor(factory);
        var options = CreateOptions() with { MaxBidirectionalStreams = 1, MaxConnectionsPerHost = 1 };

        var lease1 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        var acquire2Task = QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(acquire2Task.IsCompleted);

        actor.Tell(new QuicConnectionManagerActor.Release(lease1, CanReuse: true));

        var lease2 = await acquire2Task.WaitAsync(TimeSpan.FromSeconds(3),
            TestContext.Current.CancellationToken);

        Assert.NotNull(lease2);
        Assert.Same(lease1, lease2);
        Assert.Equal(1, factory.EstablishCount);

        await lease2.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Established_should_dispatch_pending_acquires_on_new_connection()
    {
        var factory = new MockFactory(maxStreams: 2);
        var actor = CreateActor(factory);
        var options = CreateOptions() with { MaxBidirectionalStreams = 2, MaxConnectionsPerHost = 2 };

        var lease1 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        var lease2 = await QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        var acquire3Task = QuicConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        var lease3 = await acquire3Task.WaitAsync(TimeSpan.FromSeconds(3),
            TestContext.Current.CancellationToken);

        Assert.NotNull(lease3);
        Assert.Equal(2, factory.EstablishCount);

        await lease1.DisposeAsync();
        await lease2.DisposeAsync();
        await lease3.DisposeAsync();
    }
}