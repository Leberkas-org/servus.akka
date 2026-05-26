using Akka.Actor;
using static Servus.Core.Servus;

namespace Servus.Akka.Transport.Tcp.Client;

public sealed class TcpConnectionManagerActor : ReceiveActor, IWithTimers
{
    internal sealed record Acquire(
        TransportOptions Options,
        TaskCompletionSource<ConnectionLease> Tcs,
        CancellationToken Token);

    internal sealed record Release(ConnectionLease Lease, bool CanReuse);

    private sealed record Established(ConnectionLease Lease, Acquire Original);

    private sealed record EstablishFailed(Exception Ex, Acquire Original);

    internal sealed class Evict
    {
        public static readonly Evict Instance = new();
    }

    private sealed class HostState(TransportOptions options, TcpPoolConfig config)
    {
        public readonly TransportOptions Options = options;
        public readonly TcpPoolConfig Config = config;
        public readonly List<ConnectionLease> Leases = [];
        public readonly Queue<ConnectionLease> Idle = new();
        public readonly Queue<Acquire> Pending = new();
        public int Establishing;
    }

    private readonly Dictionary<TransportOptions, HostState> _hosts = new();
    private readonly ITcpConnectionFactory _factory;
    private readonly PoolConfigRegistry _registry;
    private const string EvictTimerKey = "evict-idle";

    public ITimerScheduler Timers { get; set; } = null!;

    internal static Task<ConnectionLease> AcquireAsync(
        IActorRef actor, TransportOptions options, CancellationToken ct = default)
    {
        var tcs = new TaskCompletionSource<ConnectionLease>();

        if (ct.CanBeCanceled)
        {
            ct.UnsafeRegister(
                static (state, token) => ((TaskCompletionSource<ConnectionLease>)state!).TrySetCanceled(token),
                tcs);
        }

        actor.Tell(new Acquire(options, tcs, ct));
        return tcs.Task;
    }

    public TcpConnectionManagerActor(PoolConfigRegistry registry) : this(new TcpConnectionFactory(),
        registry)
    {
    }

    internal TcpConnectionManagerActor(ITcpConnectionFactory factory, PoolConfigRegistry registry)
    {
        _factory = factory;
        _registry = registry;

        Receive<Acquire>(OnAcquire);
        Receive<Release>(OnRelease);
        Receive<Established>(OnEstablished);
        Receive<EstablishFailed>(OnFailed);
        Receive<Evict>(_ => OnEvict());
    }

    protected override void PreStart()
    {
        Timers.StartPeriodicTimer(EvictTimerKey, Evict.Instance,
            TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
    }

    private void OnAcquire(Acquire msg)
    {
        if (msg.Tcs.Task.IsCompleted) return;

        var host = GetOrCreateHost(msg.Options);
        Tracing.For("Pool").Trace(this, "Acquire {0}:{1}", msg.Options.Host, msg.Options.Port);

        while (host.Idle.TryDequeue(out var idle))
        {
            if (idle.IsAlive() && !idle.IsExpired(host.Config.ConnectionLifetime))
            {
                if (msg.Tcs.TrySetResult(idle))
                {
                    Tracing.For("Pool").Debug(this, "Reused idle connection to {0}:{1}", msg.Options.Host, msg.Options.Port);
                    return;
                }
            }
            else
            {
                host.Leases.Remove(idle);
                idle.Dispose();
            }
        }

        if (host.Leases.Count + host.Establishing < host.Config.MaxConnectionsPerHost)
        {
            Tracing.For("Pool").Debug(this, "Creating connection to {0}:{1}", msg.Options.Host, msg.Options.Port);
            Establish(host, msg);
        }
        else
        {
            host.Pending.Enqueue(msg);
        }
    }

    private void OnRelease(Release msg)
    {
        var options = FindHostKey(msg.Lease);

        if (options is null || !_hosts.TryGetValue(options, out var host))
        {
            msg.Lease.Dispose();
            return;
        }

        Tracing.For("Pool").Trace(this, "Released {0}:{1}", options.Host, options.Port);

        if (!msg.CanReuse || !msg.Lease.IsAlive())
        {
            host.Leases.Remove(msg.Lease);
            msg.Lease.Dispose();
            ServeNextPending(host);
            return;
        }

        while (host.Pending.TryDequeue(out var pending))
        {
            if (!pending.Tcs.Task.IsCompleted)
            {
                if (pending.Tcs.TrySetResult(msg.Lease))
                {
                    return;
                }
            }
        }

        host.Idle.Enqueue(msg.Lease);
    }

    private void OnEstablished(Established msg)
    {
        var host = GetOrCreateHost(msg.Original.Options);
        host.Establishing--;
        host.Leases.Add(msg.Lease);
        Tracing.For("Pool").Debug(this, "Established to {0}:{1}", msg.Original.Options.Host, msg.Original.Options.Port);

        if (!msg.Original.Tcs.TrySetResult(msg.Lease))
        {
            OnRelease(new Release(msg.Lease, CanReuse: true));
        }
    }

    private void OnFailed(EstablishFailed msg)
    {
        if (_hosts.TryGetValue(msg.Original.Options, out var host))
        {
            host.Establishing--;
        }

        Tracing.For("Pool").Warning(this, "Failed to {0}:{1}: {2}", msg.Original.Options.Host, msg.Original.Options.Port, msg.Ex.Message);

        if (msg.Ex is OperationCanceledException oce)
        {
            msg.Original.Tcs.TrySetCanceled(oce.CancellationToken);
        }
        else
        {
            msg.Original.Tcs.TrySetException(msg.Ex);
        }

        if (host is not null)
        {
            ServeNextPending(host);
        }
    }

    private void OnEvict()
    {
        foreach (var host in _hosts.Values)
        {
            var toRemove = new List<ConnectionLease>();
            var newIdle = new Queue<ConnectionLease>();

            while (host.Idle.TryDequeue(out var lease))
            {
                if (!lease.IsAlive() || lease.IsExpired(host.Config.ConnectionLifetime))
                {
                    toRemove.Add(lease);
                }
                else
                {
                    newIdle.Enqueue(lease);
                }
            }

            while (newIdle.TryDequeue(out var kept))
            {
                host.Idle.Enqueue(kept);
            }

            foreach (var lease in toRemove)
            {
                host.Leases.Remove(lease);
                lease.Dispose();
            }
        }
    }

    protected override void PostStop()
    {
        Timers.CancelAll();
        foreach (var host in _hosts.Values)
        {
            while (host.Pending.TryDequeue(out var pending))
            {
                pending.Tcs.TrySetException(new ObjectDisposedException(
                    nameof(TcpConnectionManagerActor)));
            }

            foreach (var lease in host.Leases)
            {
                lease.Dispose();
            }
        }

        _hosts.Clear();
    }

    private TransportOptions? FindHostKey(ConnectionLease lease)
    {
        foreach (var (key, host) in _hosts)
        {
            if (host.Leases.Contains(lease))
            {
                return key;
            }
        }

        return null;
    }

    private HostState GetOrCreateHost(TransportOptions options)
    {
        if (!_hosts.TryGetValue(options, out var state))
        {
            var config = _registry.Resolve(options.PoolKey);
            state = new HostState(options, config);
            _hosts[options] = state;
        }

        return state;
    }

    private void Establish(HostState host, Acquire msg)
    {
        host.Establishing++;
        _factory
            .EstablishAsync(msg.Options, msg.Token)
            .PipeTo(Self,
                success: lease => new Established(lease, msg),
                failure: ex => new EstablishFailed(ex, msg));
    }

    private void ServeNextPending(HostState host)
    {
        while (host.Pending.TryDequeue(out var next))
        {
            if (!next.Tcs.Task.IsCompleted)
            {
                if (host.Leases.Count + host.Establishing < host.Config.MaxConnectionsPerHost)
                {
                    Establish(host, next);
                    return;
                }

                host.Pending.Enqueue(next);
                return;
            }
        }
    }
}