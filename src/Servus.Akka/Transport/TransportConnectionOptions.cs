namespace Servus.Akka.Transport;

/// <summary>
/// Construction-time options for the duplex connection types (<see cref="RawSocketConnection"/> and
/// <see cref="StreamConnection"/>). Defaults mirror <see cref="TransportOptions"/>'s Kestrel-aligned
/// <c>OutputPauseThreshold</c>/<c>OutputResumeThreshold</c> — tests that need the 512K/256K boundary
/// values (e.g. watermark-crossing specs) must pass them explicitly via the constructor.
/// </summary>
internal sealed record TransportConnectionOptions
{
    public int ReceiveBufferHint { get; init; } = 64 * 1024;
    public long OutputHighWatermark { get; init; } = 64 * 1024;
    public long OutputLowWatermark { get; init; } = 32 * 1024;

    internal static TransportConnectionOptions FromListener(ListenerOptions listener) => new()
    {
        ReceiveBufferHint = listener.ReceiveBufferHint,
        OutputHighWatermark = listener.OutputPauseThreshold,
        OutputLowWatermark = listener.OutputResumeThreshold,
    };

    internal static TransportConnectionOptions FromTransport(TransportOptions transport) => new()
    {
        ReceiveBufferHint = transport.ReceiveBufferHint,
        OutputHighWatermark = transport.OutputPauseThreshold,
        OutputLowWatermark = transport.OutputResumeThreshold,
    };
}
