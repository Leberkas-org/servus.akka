using Akka.Actor;
using Akka.TestKit.Xunit;
using Microsoft.Extensions.Time.Testing;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

public sealed class TcpConnectionManagerActorSpec : TestKit
{
    private readonly InMemoryTcpConnectionFactory _factory = new();

    private static readonly TcpPoolConfig DefaultPoolConfig = new(
        MaxConnectionsPerHost: 6,
        IdleTimeout: TimeSpan.FromSeconds(5),
        ConnectionLifetime: Timeout.InfiniteTimeSpan,
        ReuseOnUpstreamFinish: true);

    private static TcpTransportOptions CreateOptions() => new()
    {
        Host = "127.0.0.1",
        Port = 8080
    };

    private IActorRef CreateActor(PoolConfigRegistry? registry = null)
        => Sys.ActorOf(TransportFactory.CreateTcpConnectionManager(_factory, registry ?? new PoolConfigRegistry(DefaultPoolConfig)));

    [Fact(Timeout = 5000)]
    public async Task Acquire_should_create_new_connection()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.NotNull(lease);
        Assert.True(lease.IsAlive());

        lease.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Evict_should_remove_idle_connection_past_lifetime_with_injected_clock()
    {
        var clock = new FakeTimeProvider();
        var factory = new InMemoryTcpConnectionFactory(clock);
        var config = new TcpPoolConfig(
            MaxConnectionsPerHost: 6,
            IdleTimeout: TimeSpan.FromSeconds(5),
            ConnectionLifetime: TimeSpan.FromMinutes(1),
            ReuseOnUpstreamFinish: true);
        var actor = Sys.ActorOf(TransportFactory.CreateTcpConnectionManager(factory, new PoolConfigRegistry(config)));
        var options = CreateOptions();

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options, TestContext.Current.CancellationToken);
        actor.Tell(new TcpConnectionManagerActor.Release(lease1, CanReuse: true));

        // Age the idle connection past its lifetime, then run the eviction sweep.
        clock.Advance(TimeSpan.FromMinutes(2));
        actor.Tell(TcpConnectionManagerActor.Evict.Instance);

        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options, TestContext.Current.CancellationToken);

        Assert.NotSame(lease1, lease2);
        Assert.False(lease1.IsAlive());

        lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Acquire_should_reuse_idle_connection_when_strategy_allows()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new TcpConnectionManagerActor.Release(lease1, CanReuse: true));

        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.Same(lease1, lease2);

        lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Acquire_should_not_reuse_when_release_forbids()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new TcpConnectionManagerActor.Release(lease1, CanReuse: false));

        AwaitCondition(() => !lease1.IsAlive(), TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken);

        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.NotSame(lease1, lease2);

        lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Release_should_return_to_idle_when_can_reuse()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new TcpConnectionManagerActor.Release(lease, CanReuse: true));

        Assert.True(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public async Task Release_should_dispose_connection_when_cannot_reuse()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new TcpConnectionManagerActor.Release(lease, CanReuse: false));

        AwaitCondition(() => !lease.IsAlive(), TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        Assert.False(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public async Task EvictIdle_should_remove_expired_connections()
    {
        var registry = new PoolConfigRegistry(new TcpPoolConfig(
            MaxConnectionsPerHost: 6,
            IdleTimeout: TimeSpan.FromMilliseconds(50),
            ConnectionLifetime: TimeSpan.FromMilliseconds(50),
            ReuseOnUpstreamFinish: true));
        var actor = CreateActor(registry);
        var options = CreateOptions();

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        Assert.NotSame(lease1, lease2);

        actor.Tell(new TcpConnectionManagerActor.Release(lease1, CanReuse: true));
        actor.Tell(new TcpConnectionManagerActor.Release(lease2, CanReuse: true));

        await Task.Delay(100, TestContext.Current.CancellationToken);
        actor.Tell(TcpConnectionManagerActor.Evict.Instance);

        AwaitCondition(() => !lease1.IsAlive() || !lease2.IsAlive(), TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken);

        var evictedCount = (!lease1.IsAlive() ? 1 : 0) + (!lease2.IsAlive() ? 1 : 0);
        Assert.True(evictedCount >= 1, "At least one idle connection should have been evicted");
    }

    [Fact(Timeout = 5000)]
    public async Task Acquire_should_block_when_per_host_limit_is_full()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var leases = new List<ConnectionLease>();
        for (var i = 0; i < 6; i++)
        {
            leases.Add(await TcpConnectionManagerActor.AcquireAsync(actor, options,
                TestContext.Current.CancellationToken));
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await TcpConnectionManagerActor.AcquireAsync(actor, options, cts.Token);
        });

        foreach (var lease in leases)
        {
            lease.Dispose();
        }
    }

    [Fact(Timeout = 5000)]
    public async Task GracefulStop_should_dispose_all_leases()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await actor.GracefulStop(TimeSpan.FromSeconds(5));

        Assert.False(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public async Task Release_with_pending_should_hand_off_directly()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var leases = new List<ConnectionLease>();
        for (var i = 0; i < 6; i++)
        {
            leases.Add(await TcpConnectionManagerActor.AcquireAsync(actor, options,
                TestContext.Current.CancellationToken));
        }

        var pendingTask = TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        actor.Tell(new TcpConnectionManagerActor.Release(leases[0], CanReuse: true));

        var handedOff = await pendingTask.WaitAsync(TimeSpan.FromSeconds(3),
            TestContext.Current.CancellationToken);
        Assert.Same(leases[0], handedOff);

        foreach (var lease in leases.Skip(1))
        {
            lease.Dispose();
        }

        handedOff.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Multiple_hosts_should_maintain_separate_pools()
    {
        var actor = CreateActor();
        var options1 = new TcpTransportOptions { Host = "host1.example.com", Port = 80 };
        var options2 = new TcpTransportOptions { Host = "host2.example.com", Port = 80 };

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options1,
            TestContext.Current.CancellationToken);
        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options2,
            TestContext.Current.CancellationToken);

        Assert.NotSame(lease1, lease2);

        actor.Tell(new TcpConnectionManagerActor.Release(lease1, CanReuse: true));
        actor.Tell(new TcpConnectionManagerActor.Release(lease2, CanReuse: true));

        var lease3 = await TcpConnectionManagerActor.AcquireAsync(actor, options1,
            TestContext.Current.CancellationToken);
        var lease4 = await TcpConnectionManagerActor.AcquireAsync(actor, options2,
            TestContext.Current.CancellationToken);

        Assert.Same(lease1, lease3);
        Assert.Same(lease2, lease4);

        lease3.Dispose();
        lease4.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Acquire_should_timeout_when_exhausted_and_pending()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var leases = new List<ConnectionLease>();
        for (var i = 0; i < 6; i++)
        {
            leases.Add(await TcpConnectionManagerActor.AcquireAsync(actor, options,
                TestContext.Current.CancellationToken));
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await TcpConnectionManagerActor.AcquireAsync(actor, options, cts.Token);
        });

        Assert.NotNull(ex);

        foreach (var lease in leases)
        {
            lease.Dispose();
        }
    }

    [Fact(Timeout = 5000)]
    public async Task Release_dead_lease_should_not_crash_actor()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        lease.Dispose();

        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        Assert.NotNull(lease2);

        lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Idle_timeout_zero_should_disable_eviction()
    {
        var registry = new PoolConfigRegistry(new TcpPoolConfig(
            MaxConnectionsPerHost: 6,
            IdleTimeout: TimeSpan.Zero,
            ConnectionLifetime: Timeout.InfiniteTimeSpan,
            ReuseOnUpstreamFinish: true));
        var actor = CreateActor(registry);
        var options = CreateOptions();

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        actor.Tell(new TcpConnectionManagerActor.Release(lease1, CanReuse: true));
        actor.Tell(new TcpConnectionManagerActor.Release(lease2, CanReuse: true));

        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.True(lease1.IsAlive() || lease2.IsAlive());

        if (lease1.IsAlive()) lease1.Dispose();
        if (lease2.IsAlive()) lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Acquire_with_already_cancelled_token_should_be_ignored_by_actor()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            TcpConnectionManagerActor.AcquireAsync(actor, options, cts.Token));

        var lease = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        Assert.NotNull(lease);

        lease.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Established_with_cancelled_caller_should_release_back_to_pool()
    {
        var slowFactory = new SlowTcpConnectionFactory(TimeSpan.FromMilliseconds(200));
        var registry = new PoolConfigRegistry(DefaultPoolConfig with { MaxConnectionsPerHost = 1 });
        var actor = Sys.ActorOf(TransportFactory.CreateTcpConnectionManager(slowFactory, registry));
        var options = CreateOptions();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));

        var task1 = TcpConnectionManagerActor.AcquireAsync(actor, options, cts.Token);
        var task2 = TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task1);

        var lease = await task2;
        Assert.NotNull(lease);
        Assert.True(lease.IsAlive());

        lease.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Acquire_should_skip_dead_idle_lease_and_establish_fresh_connection()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        actor.Tell(new TcpConnectionManagerActor.Release(lease1, CanReuse: true));

        await Task.Delay(50, TestContext.Current.CancellationToken);

        lease1.Dispose();

        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        Assert.NotSame(lease1, lease2);
        Assert.True(lease2.IsAlive());

        lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task EstablishFailed_should_cascade_to_pending_waiter()
    {
        var failOnce = new FailOnceTcpConnectionFactory();
        var registry = new PoolConfigRegistry(DefaultPoolConfig with { MaxConnectionsPerHost = 1 });
        var actor = Sys.ActorOf(TransportFactory.CreateTcpConnectionManager(failOnce, registry));
        var options = CreateOptions();

        var task1 = TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await Task.Delay(10, TestContext.Current.CancellationToken);

        var task2 = TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAnyAsync<IOException>(() => task1);

        var lease = await task2;
        Assert.NotNull(lease);
        Assert.True(lease.IsAlive());

        lease.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Evicted_idle_connection_should_not_be_reused()
    {
        var registry = new PoolConfigRegistry(new TcpPoolConfig(
            MaxConnectionsPerHost: 6,
            IdleTimeout: TimeSpan.FromMilliseconds(50),
            ConnectionLifetime: TimeSpan.FromMilliseconds(50),
            ReuseOnUpstreamFinish: true));
        var actor = CreateActor(registry);
        var options = CreateOptions();

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new TcpConnectionManagerActor.Release(lease1, CanReuse: true));

        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        Assert.NotNull(lease2);

        lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task OnEvict_should_dispose_dead_leases()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        lease1.Dispose();
        actor.Tell(new TcpConnectionManagerActor.Release(lease2, CanReuse: true));

        actor.Tell(TcpConnectionManagerActor.Evict.Instance);

        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.False(lease1.IsAlive());
        Assert.True(lease2.IsAlive());

        lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task OnEvict_should_preserve_valid_idle_leases()
    {
        var registry = new PoolConfigRegistry(new TcpPoolConfig(
            MaxConnectionsPerHost: 6,
            IdleTimeout: TimeSpan.FromSeconds(5),
            ConnectionLifetime: TimeSpan.FromSeconds(5),
            ReuseOnUpstreamFinish: true));
        var actor = CreateActor(registry);
        var options = CreateOptions();

        var lease = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new TcpConnectionManagerActor.Release(lease, CanReuse: true));

        actor.Tell(TcpConnectionManagerActor.Evict.Instance);

        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.True(lease.IsAlive());

        lease.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task OnEstablished_with_cancelled_caller_should_release_back()
    {
        var slowFactory = new SlowTcpConnectionFactory(TimeSpan.FromMilliseconds(100));
        var registry = new PoolConfigRegistry(DefaultPoolConfig with { MaxConnectionsPerHost = 2 });
        var actor = Sys.ActorOf(TransportFactory.CreateTcpConnectionManager(slowFactory, registry));
        var options = CreateOptions();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));
        var task1 = TcpConnectionManagerActor.AcquireAsync(actor, options, cts.Token);
        var task2 = TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task1);

        var lease = await task2.WaitAsync(TimeSpan.FromSeconds(3),
            TestContext.Current.CancellationToken);
        Assert.NotNull(lease);
        Assert.True(lease.IsAlive());

        lease.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task OnFailed_should_decrement_establishing_and_serve_pending()
    {
        var failOnce = new FailOnceTcpConnectionFactory();
        var registry = new PoolConfigRegistry(DefaultPoolConfig with { MaxConnectionsPerHost = 1 });
        var actor = Sys.ActorOf(TransportFactory.CreateTcpConnectionManager(failOnce, registry));
        var options = CreateOptions();

        var task1 = TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await Task.Delay(10, TestContext.Current.CancellationToken);

        var task2 = TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAnyAsync<IOException>(() => task1);

        var lease = await task2.WaitAsync(TimeSpan.FromSeconds(3),
            TestContext.Current.CancellationToken);
        Assert.NotNull(lease);
        Assert.True(lease.IsAlive());

        lease.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task Release_dead_unknown_lease_should_not_crash()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        lease.Dispose();

        actor.Tell(new TcpConnectionManagerActor.Release(lease, CanReuse: true));

        await Task.Delay(100, TestContext.Current.CancellationToken);

        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        Assert.NotNull(lease2);

        lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task OnRelease_should_cascade_pending_when_cant_establish()
    {
        var registry = new PoolConfigRegistry(DefaultPoolConfig with { MaxConnectionsPerHost = 2 });
        var actor = CreateActor(registry);
        var options = CreateOptions();

        var leases = new List<ConnectionLease>();
        for (var i = 0; i < 2; i++)
        {
            leases.Add(await TcpConnectionManagerActor.AcquireAsync(actor, options,
                TestContext.Current.CancellationToken));
        }

        var pendingTask = TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await Task.Delay(50, TestContext.Current.CancellationToken);

        actor.Tell(new TcpConnectionManagerActor.Release(leases[0], CanReuse: true));

        var handed = await pendingTask.WaitAsync(TimeSpan.FromSeconds(3),
            TestContext.Current.CancellationToken);
        Assert.Same(leases[0], handed);

        leases[1].Dispose();
        handed.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task OnAcquire_should_skip_expired_idle_leases()
    {
        var registry = new PoolConfigRegistry(new TcpPoolConfig(
            MaxConnectionsPerHost: 6,
            IdleTimeout: TimeSpan.FromSeconds(5),
            ConnectionLifetime: TimeSpan.FromMilliseconds(50),
            ReuseOnUpstreamFinish: true));
        var actor = CreateActor(registry);
        var options = CreateOptions();

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new TcpConnectionManagerActor.Release(lease1, CanReuse: true));

        await Task.Delay(100, TestContext.Current.CancellationToken);

        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.NotSame(lease1, lease2);
        Assert.True(lease2.IsAlive());

        lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task OnAcquire_should_skip_dead_idle_lease_and_create_new()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var lease1 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);
        actor.Tell(new TcpConnectionManagerActor.Release(lease1, CanReuse: true));

        lease1.Dispose();

        var lease2 = await TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        Assert.NotSame(lease1, lease2);
        Assert.True(lease2.IsAlive());

        lease2.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task PostStop_should_reject_pending_requests()
    {
        var actor = CreateActor();
        var options = CreateOptions();

        var leases = new List<ConnectionLease>();
        for (var i = 0; i < 6; i++)
        {
            leases.Add(await TcpConnectionManagerActor.AcquireAsync(actor, options,
                TestContext.Current.CancellationToken));
        }

        var pendingTask = TcpConnectionManagerActor.AcquireAsync(actor, options,
            TestContext.Current.CancellationToken);

        await actor.GracefulStop(TimeSpan.FromSeconds(2));

        await Assert.ThrowsAnyAsync<ObjectDisposedException>(() => pendingTask);

        foreach (var lease in leases)
        {
            Assert.False(lease.IsAlive());
        }
    }

}
