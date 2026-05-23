using Akka.Actor;
using Akka.Event;

namespace Servus.Akka.Local;

public class LocalEntityRegionActor : ReceiveActor, IWithUnboundedStash
{
    private sealed record Initialize;
    private sealed record PassivationTick;

    private static readonly char[] InvalidEntityIdChars = ['/', '#', '$'];

    private readonly Func<string, Props> _entityPropsFactory;
    private readonly IEntityIdExtractor _messageExtractor;
    private readonly IEntityIdStore _entityIdStore;
    private readonly TimeSpan? _passivateAfter;

    private readonly Dictionary<string, IActorRef> _entities = [];
    private readonly Dictionary<string, DateTime> _lastActivity = [];
    private readonly HashSet<string> _passivating = [];

    private readonly ILoggingAdapter _log = Context.GetLogger();
    private ICancelable? _passivationTimer;

    public IStash Stash { get; set; } = null!;

    public LocalEntityRegionActor(
        Func<string, Props> entityPropsFactory,
        IEntityIdExtractor messageExtractor,
        LocalEntityRegionOptions? options = null)
    {
        _entityPropsFactory = entityPropsFactory;
        _messageExtractor = messageExtractor;
        _entityIdStore = options?.EntityIdStore ?? new InMemoryEntityIdStore();
        _passivateAfter = options?.PassivateIdleEntityAfter;

        Initializing();
    }

    protected override void PreStart()
    {
        base.PreStart();
        Self.Tell(new Initialize());
    }

    protected override void PostStop()
    {
        _passivationTimer?.Cancel();
        base.PostStop();
    }

    private void Initializing()
    {
        ReceiveAsync<Initialize>(async _ =>
        {
            var entities = await _entityIdStore.LoadEntitiesAsync();
            foreach (var entityId in entities)
            {
                SpawnEntity(entityId);
                _log.Info("Entity [{0}] recovered", entityId);
            }

            Become(Ready);
            Stash.UnstashAll();
            SchedulePassivation();
        });
        ReceiveAny(_ => Stash.Stash());
    }

    private void Ready()
    {
        ReceiveAsync<PassivationTick>(async _ => await RunPassivationAsync());
        ReceiveAsync<Terminated>(async t => await HandleTerminatedAsync(t));
        ReceiveAsync<object>(async msg => await RouteMessageAsync(msg));
    }

    private async Task RouteMessageAsync(object message)
    {
        var entityId = _messageExtractor.EntityId(message);
        if (entityId is null)
        {
            Unhandled(message);
            return;
        }

        if (!IsValidEntityId(entityId))
        {
            _log.Warning("Invalid entity ID [{0}] — must not be empty or contain '/', '#', '$'", entityId);
            Unhandled(message);
            return;
        }

        if (_passivating.Contains(entityId))
        {
            Stash.Stash();
            return;
        }

        var entityRef = await GetOrCreateEntityAsync(entityId);
        _lastActivity[entityId] = DateTime.UtcNow;
        entityRef.Forward(_messageExtractor.EntityMessage(message));
    }

    private async Task<IActorRef> GetOrCreateEntityAsync(string entityId)
    {
        if (_entities.TryGetValue(entityId, out var existing))
            return existing;

        var child = SpawnEntity(entityId);
        await _entityIdStore.EntityStarted(entityId);
        _log.Info("Entity [{0}] started", entityId);
        return child;
    }

    private IActorRef SpawnEntity(string entityId)
    {
        var props = _entityPropsFactory(entityId);
        var child = Context.ActorOf(props, entityId);
        Context.Watch(child);

        _entities[entityId] = child;
        _lastActivity[entityId] = DateTime.UtcNow;
        return child;
    }

    private async Task HandleTerminatedAsync(Terminated terminated)
    {
        var entityId = terminated.ActorRef.Path.Name;

        if (_passivating.Remove(entityId))
        {
            _entities.Remove(entityId);
            _lastActivity.Remove(entityId);
            Stash.UnstashAll();
            return;
        }

        _entities.Remove(entityId);
        _lastActivity.Remove(entityId);

        var child = SpawnEntity(entityId);
        await _entityIdStore.EntityStarted(entityId);
        _log.Warning("Entity [{0}] restarted after unexpected termination", entityId);
    }

    private async Task RunPassivationAsync()
    {
        if (_passivateAfter is not { } timeout)
            return;

        var cutoff = DateTime.UtcNow - timeout;
        var toPassivate = _lastActivity
            .Where(kv => kv.Value < cutoff)
            .Select(kv => kv.Key)
            .ToList();

        foreach (var entityId in toPassivate)
        {
            if (!_entities.TryGetValue(entityId, out var actorRef))
                continue;

            _passivating.Add(entityId);
            await _entityIdStore.EntityStopped(entityId);
            _log.Info("Entity [{0}] passivating (idle for {1})", entityId, _passivateAfter);
            Context.Stop(actorRef);
        }
    }

    private void SchedulePassivation()
    {
        if (_passivateAfter is not { } timeout)
            return;

        var interval = TimeSpan.FromSeconds(Math.Max(timeout.TotalSeconds / 2, 1));
        _passivationTimer = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(
            interval, interval, Self, new PassivationTick(), ActorRefs.NoSender);
    }

    public static bool IsValidEntityId(string entityId)
    {
        return !string.IsNullOrWhiteSpace(entityId)
               && entityId.AsSpan().IndexOfAny(InvalidEntityIdChars) < 0;
    }
}
