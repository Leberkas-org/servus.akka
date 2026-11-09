using Akka.Actor;
using Akka.Event;
using Akka.Util.Internal;

namespace Servus.Akka.Local;

public class LocalEntityRegionActor : ReceiveActor, IWithUnboundedStash
{
    private sealed record PassivationTick;

    private const int MaxRestarts = 3;
    private static readonly char[] InvalidEntityIdChars = ['/', '#', '$'];

    private readonly Func<string, Props> _entityPropsFactory;
    private readonly IEntityIdExtractor _messageExtractor;
    private readonly IEntityIdStore _entityIdStore;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan? _passivateAfter;
    private readonly object _stopMessage;

    private readonly Dictionary<string, IActorRef> _entities = [];
    private readonly Dictionary<string, DateTime> _lastActivity = [];
    private readonly Dictionary<string, int> _restartCounts = [];
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
        _timeProvider = options?.TimeProvider ?? TimeProvider.System;
        _passivateAfter = options?.PassivateIdleEntityAfter;
        _stopMessage = options?.StopMessage ?? PoisonPill.Instance;

        Initializing();
    }

    protected override void PreStart()
    {
        base.PreStart();
        _entityIdStore.LoadEntitiesAsync().PipeTo(Self);
    }

    protected override void PostStop()
    {
        _passivationTimer?.Cancel();
        base.PostStop();
    }

    private void Initializing()
    {
        Receive<IReadOnlyCollection<string>>(entities =>
        {
            entities.ForEach(e =>
            {
                SpawnEntity(e, false);
                _log.Info("Entity [{0}] recovered", e);
            });

            BecomeReady();
        });

        Receive<Status.Failure>(f =>
        {
            _log.Error(f.Cause, "Failed to load entities from store");
            BecomeReady();
        });

        ReceiveAny(_ => Stash.Stash());
    }

    private void BecomeReady()
    {
        Become(() =>
        {
            Receive<PassivationTick>(_ => RunPassivation());
            Receive<Passivate>(HandlePassivate);
            Receive<Terminated>(HandleTerminated);
            Receive<Status.Failure>(f => _log.Error(f.Cause, "Entity store operation failed"));
            Receive<object>(RouteMessage);
        });
        
        Stash.UnstashAll();
        SchedulePassivation();
    }

    private void HandlePassivate(Passivate passivate)
    {
        var entityId = Sender.Path.Name;
        if (!_entities.TryGetValue(entityId, out var entity) || !entity.Equals(Sender))
        {
            _log.Warning("Ignoring Passivate from [{0}]: only entities of this region can passivate", Sender);
            return;
        }

        if (!_passivating.Contains(entityId))
            BeginPassivation(entityId, entity, passivate.StopMessage, "requested by the entity");
    }

    /// <summary>
    /// Stops delivering to the entity and asks it to stop with <paramref name="stopMessage"/>. Messages for it are
    /// buffered until it terminated, then it is started again for them (see <see cref="HandleTerminated"/>).
    /// </summary>
    private void BeginPassivation(string entityId, IActorRef entity, object stopMessage, string reason)
    {
        _passivating.Add(entityId);
        _entityIdStore.EntityStopped(entityId).PipeTo(Self);
        _log.Info("Entity [{0}] passivating ({1})", entityId, reason);
        entity.Tell(stopMessage, ActorRefs.NoSender);
    }

    private void RouteMessage(object message)
    {
        var entityId = _messageExtractor.EntityId(message);
        if (entityId is null || !IsValidEntityId(entityId))
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

        var entityRef = GetOrCreateEntity(entityId);
        _restartCounts.Remove(entityId);
        _lastActivity[entityId] = _timeProvider.GetUtcNow().UtcDateTime;
        entityRef.Forward(_messageExtractor.EntityMessage(message));
    }

    private IActorRef GetOrCreateEntity(string entityId)
    {
        if (_entities.TryGetValue(entityId, out var existing))
            return existing;

        var child = SpawnEntity(entityId);
        _log.Info("Entity [{0}] started", entityId);
        return child;
    }

    private IActorRef SpawnEntity(string entityId, bool persist = true)
    {
        if(persist) _entityIdStore.EntityStarted(entityId).PipeTo(Self);
        
        var props = _entityPropsFactory(entityId);
        var child = Context.ActorOf(props, entityId);
        Context.Watch(child);

        _entities[entityId] = child;
        _lastActivity[entityId] = _timeProvider.GetUtcNow().UtcDateTime;
        return child;
    }

    private void HandleTerminated(Terminated terminated)
    {
        var entityId = terminated.ActorRef.Path.Name;

        _entities.Remove(entityId);
        _lastActivity.Remove(entityId);

        if (_passivating.Remove(entityId))
        {
            Stash.UnstashAll();
            return;
        }

        if (Context.System.WhenTerminated.IsCompleted)
            return;

        var count = _restartCounts.GetValueOrDefault(entityId, 0) + 1;
        if (count > MaxRestarts)
        {
            _log.Error("Entity [{0}] exceeded max restart attempts ({1}), giving up", entityId, MaxRestarts);
            _restartCounts.Remove(entityId);
            return;
        }

        _restartCounts[entityId] = count;
        SpawnEntity(entityId);
        _log.Warning("Entity [{0}] restarted after unexpected termination (attempt {1}/{2})", entityId, count, MaxRestarts);
    }

    private void RunPassivation()
    {
        if (_passivateAfter is not { } timeout)
            return;

        var cutoff = _timeProvider.GetUtcNow().UtcDateTime - timeout;
        var toPassivate = _lastActivity
            .Where(kv => kv.Value < cutoff)
            .Select(kv => kv.Key)
            .ToList();

        foreach (var entityId in toPassivate)
        {
            if (_passivating.Contains(entityId) || !_entities.TryGetValue(entityId, out var actorRef))
                continue;

            BeginPassivation(entityId, actorRef, _stopMessage, $"idle for {timeout}");
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
