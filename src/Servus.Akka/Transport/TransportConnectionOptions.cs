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
    public int MaxSyncReads { get; init; } = 8;
    public int? MaxBytesPerSend { get; init; }
    public int CoalesceThreshold { get; init; } = 16 * 1024;

    /// <summary>
    /// Outbound channel bound used only for QUIC-per-stream connections (<see cref="StreamConnection"/>
    /// when <c>quicAware</c> is set) as a POC safety net against unbounded credit-accounting bugs. TCP
    /// (<see cref="RawSocketConnection"/>) always passes <c>0</c> (unbounded) — it is shared by H2/H1
    /// whose in-flight count legitimately exceeds a tight bound and would false-trip.
    /// </summary>
    public int OutboundChannelCapacity { get; init; } = 64;

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

    internal static TransportConnectionOptions FromQuicTransport(TransportOptions transport) => new()
    {
        ReceiveBufferHint = transport.ReceiveBufferHint,
        OutputHighWatermark = transport.OutputPauseThreshold,
        OutputLowWatermark = transport.OutputResumeThreshold,
        CoalesceThreshold = 4 * 1024,
    };

    internal static TransportConnectionOptions FromQuicListener(ListenerOptions listener) => new()
    {
        ReceiveBufferHint = listener.ReceiveBufferHint,
        OutputHighWatermark = listener.OutputPauseThreshold,
        OutputLowWatermark = listener.OutputResumeThreshold,
        CoalesceThreshold = 4 * 1024,
    };
}
