using Akka.Actor;
using static Servus.Core.Servus;

namespace Servus.Akka.Transport.Quic.Client;

public sealed class QuicConnectionManagerActor : ReceiveActor, IWithTimers
{
    internal sealed record Acquire(
        QuicTransportOptions Options,
        TaskCompletionSource<QuicConnectionLease> Tcs,
        CancellationToken Token);

    internal sealed record Release(QuicConnectionLease Lease, bool CanReuse);

    private sealed record Established(QuicConnectionLease Lease, Acquire Original);

    private sealed record EstablishFailed(Exception Ex, Acquire Original);

    internal sealed class Evict
    {
        public static readonly Evict Instance = new();
    }

    private sealed class HostState(int maxConnections)
    {
        public readonly int MaxConnections = maxConnections;
        public readonly List<QuicConnectionLease> Leases = [];
        public readonly Queue<Acquire> Pending = new();
        public int Establishing;
    }

    private readonly Dictionary<TransportOptions, HostState> _hosts = new();
    private readonly IQuicConnectionFactory _factory;
    private const string EvictTimerKey = "evict-idle";

    public ITimerScheduler Timers { get; set; } = null!;

    internal static Task<QuicConnectionLease> AcquireAsync(
        IActorRef actor, QuicTransportOptions options, CancellationToken ct = default)
    {
        var tcs = new TaskCompletionSource<QuicConnectionLease>();
        if (ct.CanBeCanceled)
        {
            ct.UnsafeRegister(
                static (state, token) => ((TaskCompletionSource<QuicConnectionLease>)state!).TrySetCanceled(token),
                tcs);
        }

        actor.Tell(new Acquire(options, tcs, ct));
        return tcs.Task;
    }

    public QuicConnectionManagerActor() : this(new QuicConnectionFactory())
    {
    }

    internal QuicConnectionManagerActor(IQuicConnectionFactory factory)
    {
        _factory = factory;
        Receive<Acquire>(OnAcquire);
        ReceiveAsync<Release>(OnRelease);
        ReceiveAsync<Established>(OnEstablished);
        Receive<EstablishFailed>(OnFailed);
        ReceiveAsync<Evict>(_ => OnEvict());
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

        foreach (var lease in host.Leases)
        {
            if (!lease.CanAcceptStream() || lease.IsExpired(msg.Options.ConnectionLifetime))
            {
                continue;
            }

            lease.MarkBusy();
            if (msg.Tcs.TrySetResult(lease))
            {
                Tracing.For("Pool").Debug(this, "Reused connection to {0}:{1}", msg.Options.Host, msg.Options.Port);
                return;
            }

            lease.MarkIdle();
        }

        if (host.Leases.Count + host.Establishing < host.MaxConnections)
        {
            Tracing.For("Pool").Debug(this, "Creating connection to {0}:{1}", msg.Options.Host, msg.Options.Port);
            Establish(host, msg);
        }
        else
        {
            host.Pending.Enqueue(msg);
        }
    }

    private async Task OnRelease(Release msg)
    {
        Tracing.For("Pool").Trace(this, "Released {0}", msg.Lease);
        msg.Lease.MarkIdle();

        if (!msg.CanReuse || !msg.Lease.IsAlive())
        {
            foreach (var host in _hosts.Values)
            {
                if (host.Leases.Remove(msg.Lease))
                {
                    break;
                }
            }

            if (msg.Lease.ActiveStreams == 0)
            {
                await msg.Lease.DisposeAsync();
            }
        }
    }

    private async Task OnEstablished(Established msg)
    {
        var host = GetOrCreateHost(msg.Original.Options);
        host.Establishing--;
        host.Leases.Add(msg.Lease);
        msg.Lease.MarkBusy();
        Tracing.For("Pool").Debug(this, "Established to {0}:{1}", msg.Original.Options.Host, msg.Original.Options.Port);

        if (!msg.Original.Tcs.TrySetResult(msg.Lease))
        {
            await OnRelease(new Release(msg.Lease, CanReuse: true));
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
    }

    private async Task OnEvict()
    {
        foreach (var host in _hosts.Values)
        {
            var toRemove = host.Leases
                .Where(l => !l.IsAlive() || (l.ActiveStreams == 0 && l.IsExpired(TimeSpan.FromMinutes(10))))
                .ToList();

            foreach (var lease in toRemove)
            {
                host.Leases.Remove(lease);
                await lease.DisposeAsync();
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
                pending.Tcs.TrySetException(new ObjectDisposedException(nameof(QuicConnectionManagerActor)));
            }

            foreach (var lease in host.Leases)
            {
                _ = lease.DisposeAsync();
            }
        }

        _hosts.Clear();
    }

    private HostState GetOrCreateHost(QuicTransportOptions options)
    {
        if (!_hosts.TryGetValue(options, out var state))
        {
            state = new HostState(options.MaxConnectionsPerHost);
            _hosts[options] = state;
        }

        return state;
    }

    private void Establish(HostState host, Acquire msg)
    {
        host.Establishing++;
        _factory.EstablishAsync(msg.Options, msg.Token)
            .PipeTo(Self,
                success: lease => new Established(lease, msg),
                failure: ex => new EstablishFailed(ex, msg));
    }
}