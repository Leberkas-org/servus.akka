namespace Servus.Akka.Transport.Tcp;

/// <summary>
/// One instance per (connection, generation). The transform delegates capture only this immutable
/// pair, so <c>PipeTo</c> allocates nothing per read — the closures move from per-read to
/// per-connection (same model as <see cref="Quic.QuicStreamState"/>'s cached transforms). Refreshed
/// once per generation by the state machine when a lease is acquired or the transport is cleaned up.
/// </summary>
internal sealed class ReadEventState(int gen)
{
    public readonly Func<WireBuffer?, ITcpTransportEvent> ReadSuccess = buffer => new ReadCompleted(buffer, gen);
    public readonly Func<Exception, ITcpTransportEvent> ReadFailure = ex => new ReadFailed(ex, gen);
}
