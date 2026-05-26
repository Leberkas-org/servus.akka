using Servus.Akka.Transport;

namespace Servus.Akka.TestKit;

public interface IStageContext
{
    void Push(ITransportInbound inbound);
    void Complete();
    void Fail(Exception ex);
    void ScheduleTimer(string key, TimeSpan delay);
    void CancelTimer(string key);
}
