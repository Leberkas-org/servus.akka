using Akka.Actor;

namespace Servus.Akka.Local;

public class LocalEntityRegionOptions
{
    public TimeSpan? PassivateIdleEntityAfter { get; set; }
    public IEntityIdStore? EntityIdStore { get; set; }
    public TimeProvider? TimeProvider { get; set; }

    /// <summary>
    /// The message an idle entity is sent to stop it (default <see cref="PoisonPill"/>). It is queued behind the
    /// messages the entity already has, so they are still handled. Persistent entities should use their own message
    /// and stop themselves on it, because a <see cref="PoisonPill"/> is not held back while an event is being persisted.
    /// </summary>
    public object StopMessage { get; set; } = PoisonPill.Instance;
}
