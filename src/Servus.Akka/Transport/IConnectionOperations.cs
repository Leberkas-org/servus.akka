namespace Servus.Akka.Transport;

public interface IConnectionOperations
{
    void OnPushInbound(ITransportInbound item);
    void OnSignalPullOutbound();
    void OnCompleteStage();
    void OnScheduleTimer(string key, TimeSpan delay);
    void OnCancelTimer(string key);
}