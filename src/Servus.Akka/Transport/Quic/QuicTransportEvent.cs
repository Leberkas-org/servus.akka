using System.Net;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Transport.Quic;

internal interface IQuicTransportEvent;

internal readonly record struct ConnectionLeaseAcquired(QuicConnectionLease Lease) : IQuicTransportEvent;

internal readonly record struct StreamLeaseAcquired(Stream Stream, long StreamId) : IQuicTransportEvent;

internal readonly record struct AcquisitionFailed(Exception Error) : IQuicTransportEvent;

/// <summary>
/// The background inbound-stream accept loop terminated with a non-cancellation fault (e.g. a
/// connection-level <see cref="System.Net.Quic.QuicException"/>). Routed to the actor so the
/// failure surfaces as a connection failure (and triggers reconnect) instead of vanishing as an
/// unobserved task exception.
/// </summary>
internal readonly record struct AcceptLoopFailed(Exception Error) : IQuicTransportEvent;

// The read-completion events carry the QuicStreamState itself (plus the rent Epoch it was armed under)
// rather than a stream id + generation: the cached transforms that produce them
// (QuicStreamState.ReadSuccess/ReadFailure) run on IO-completion threads and must not touch any mutable
// state, so all buffer/lifecycle handling happens on the actor via State (actor-confined, no fences).
// The Epoch lets the actor drop a completion whose state has since been repooled and re-rented for a NEW
// stream — a stale read can never be pushed as the re-rented stream's data or tear it down.

/// <summary>
/// A stream receive completed. <see cref="Buffer"/> carries the freshly received, caller-owned
/// <see cref="WireBuffer"/> (ownership transfers to the handler) or null on EOF. <see cref="Epoch"/> is
/// the <see cref="QuicStreamState.Epoch"/> the read was armed under.
/// </summary>
internal sealed class StreamReceiveCompleted : IQuicTransportEvent
{
    public QuicStreamState State { get; }
    public WireBuffer? Buffer { get; }
    public int Epoch { get; }

    public StreamReceiveCompleted(QuicStreamState state, WireBuffer? buffer, int epoch)
    {
        State = state;
        Buffer = buffer;
        Epoch = epoch;
    }
}

/// <summary>
/// A stream receive faulted; the exception is classified on the actor. <see cref="Epoch"/> is the
/// <see cref="QuicStreamState.Epoch"/> the read was armed under.
/// </summary>
internal sealed class StreamReceiveFailed : IQuicTransportEvent
{
    public QuicStreamState State { get; }
    public Exception Error { get; }
    public int Epoch { get; }

    public StreamReceiveFailed(QuicStreamState state, Exception error, int epoch)
    {
        State = state;
        Error = error;
        Epoch = epoch;
    }
}

internal readonly record struct InboundStreamAccepted(Stream Stream, long StreamId) : IQuicTransportEvent;

internal readonly record struct MigrationDetected(EndPoint OldEndPoint, EndPoint NewEndPoint) : IQuicTransportEvent;
