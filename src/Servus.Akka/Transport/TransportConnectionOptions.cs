namespace Servus.Akka.Transport;

/// <summary>
/// Construction-time options for the new duplex connection types (<see cref="RawSocketConnection"/>
/// and, later, the QUIC equivalent). The watermarks are consumed by the state machines in a later
/// phase — they are carried here for construction symmetry and are currently unused by the transport.
/// </summary>
internal sealed record TransportConnectionOptions
{
    public int ReceiveBufferHint { get; init; } = 64 * 1024;
    public long OutputHighWatermark { get; init; } = 512 * 1024;
    public long OutputLowWatermark { get; init; } = 256 * 1024;

    internal static TransportConnectionOptions FromListener(ListenerOptions listener) => new()
    {
        ReceiveBufferHint = listener.ReceiveBufferHint,
        OutputHighWatermark = listener.OutputPauseThreshold,
        OutputLowWatermark = listener.OutputResumeThreshold,
    };
}
