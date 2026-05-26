using Akka.Event;
using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Utils;

internal sealed class MockTransportOperations : ITransportOperations
{
    public List<ITransportInbound> PushedInbound { get; } = [];
    public int PullOutboundCount { get; set; }
    public int CompleteStageCount { get; set; }
    public List<(string Key, TimeSpan Delay)> ScheduledTimers { get; } = [];
    public List<string> CancelledTimers { get; } = [];

    public void OnPushInbound(ITransportInbound item) => PushedInbound.Add(item);
    public void OnSignalPullOutbound() => PullOutboundCount++;
    public void OnCompleteStage() => CompleteStageCount++;
    public void OnScheduleTimer(string key, TimeSpan delay) => ScheduledTimers.Add((key, delay));
    public void OnCancelTimer(string key) => CancelledTimers.Add(key);
    public ILoggingAdapter Log => NoLogger.Instance;
}
