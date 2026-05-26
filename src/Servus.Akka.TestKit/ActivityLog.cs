using Servus.Akka.Transport;

namespace Servus.Akka.TestKit;

public abstract record Activity
{
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}

public sealed record OutboundReceived(int Index, ITransportOutbound Message) : Activity;

public sealed record InboundPushed(int Index, ITransportInbound Message) : Activity;

public sealed record HandlerInvoked(string HandlerType, ITransportOutbound Trigger) : Activity;

public sealed record StageCompleted : Activity;

public sealed record StageFailed(Exception Exception) : Activity;

public sealed class ActivityLog
{
    private readonly List<Activity> _entries = [];

    public IReadOnlyList<Activity> Entries => _entries;

    public void Record(Activity activity) => _entries.Add(activity);

    public IEnumerable<T> OfType<T>() where T : Activity
        => _entries.OfType<T>();

    public void Clear() => _entries.Clear();
}

public sealed record ListenerConnectionAccepted(int Index, bool FromFactory) : Activity;
