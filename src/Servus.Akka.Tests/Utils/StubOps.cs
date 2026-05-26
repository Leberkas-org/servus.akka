using Akka.Event;
using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Utils;

internal sealed class StubOps : ITransportOperations
{
    public readonly List<ITransportInbound> PushedInbound = [];
    public int PullCount;
    public bool Completed;
    public readonly Dictionary<string, TimeSpan> Timers = new();
    public readonly HashSet<string> CancelledTimers = [];

    public void OnPushInbound(ITransportInbound item) => PushedInbound.Add(item);
    public void OnSignalPullOutbound() => PullCount++;
    public void OnCompleteStage() => Completed = true;
    public void OnScheduleTimer(string key, TimeSpan delay) => Timers[key] = delay;
    public void OnCancelTimer(string key) => CancelledTimers.Add(key);
    public ILoggingAdapter Log => NoLogger.Instance;
}