using System.Net;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Transport.Quic;

internal interface IQuicTransportEvent;

internal readonly record struct ConnectionLeaseAcquired(QuicConnectionLease Lease) : IQuicTransportEvent;

internal readonly record struct StreamLeaseAcquired(Stream Stream, long StreamId) : IQuicTransportEvent;

internal readonly record struct AcquisitionFailed(Exception Error) : IQuicTransportEvent;

// The read-completion events carry the QuicStreamState itself rather than a stream id + generation:
// the cached transforms that produce them (QuicStreamState.ReadSuccess/ReadFailure) run on IO-completion
// threads and must not touch any mutable state, so all buffer/lifecycle handling happens on the actor via
// State (actor-confined, no fences). Carrying the state also makes stale-read disambiguation structural —
// a reconnect's reused stream ids can never be confused with a previous connection's in-flight read.

/// <summary>
/// A stream receive completed. <see cref="Buffer"/> carries the freshly received, caller-owned
/// <see cref="WireBuffer"/> (ownership transfers to the handler) or null on EOF.
/// </summary>
internal readonly record struct StreamReceiveCompleted(QuicStreamState State, WireBuffer? Buffer) : IQuicTransportEvent;

/// <summary>A stream receive faulted; the exception is classified on the actor.</summary>
internal readonly record struct StreamReceiveFailed(QuicStreamState State, Exception Error) : IQuicTransportEvent;

internal readonly record struct InboundStreamAccepted(Stream Stream, long StreamId) : IQuicTransportEvent;

internal readonly record struct MigrationDetected(EndPoint OldEndPoint, EndPoint NewEndPoint) : IQuicTransportEvent;
