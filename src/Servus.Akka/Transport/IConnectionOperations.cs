namespace Servus.Akka.Transport;

internal interface IConnectionOperations
{
    /// <summary>
    /// Pushes an inbound item toward the stage's outlet. Returns true when the item was pushed
    /// immediately (downstream was pulled); false when it was queued because downstream was not ready.
    /// QUIC's per-stream read pump uses this to decide whether to re-arm the stream's read right away
    /// (pushed) or defer re-arming to the dequeue site (queued) — see <c>QuicStreamReads</c>.
    /// </summary>
    bool OnPushInbound(ITransportInbound item);
    void OnSignalPullOutbound();
    void OnCompleteStage();
    void OnScheduleTimer(string key, TimeSpan delay);
    void OnCancelTimer(string key);
}