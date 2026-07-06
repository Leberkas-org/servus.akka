using System.IO.Pipelines;
using System.Net;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Transport.Quic;

internal interface IQuicTransportEvent;

internal readonly record struct ConnectionLeaseAcquired(QuicConnectionLease Lease) : IQuicTransportEvent;

internal readonly record struct StreamLeaseAcquired(Stream Stream, long StreamId) : IQuicTransportEvent;

internal readonly record struct AcquisitionFailed(Exception Error) : IQuicTransportEvent;

// The read-completion events carry the QuicStreamState itself rather than a stream id + generation:
// the PipeTo transforms that produce them run on IO-completion threads and must not touch any
// mutable state, so all buffer/lifecycle handling happens on the actor via State (actor-confined,
// no fences). Carrying the state also makes stale-read disambiguation structural — a reconnect's
// reused stream ids can never be confused with a previous connection's in-flight read.

/// <summary>A QuicStream.ReadAsync completed; the data sits in State's pending read buffer.</summary>
internal readonly record struct DirectStreamReadComplete(QuicStreamState State, int BytesRead) : IQuicTransportEvent;

/// <summary>A PipeReader.ReadAsync completed; Result is consumed (copy + AdvanceTo) on the actor.</summary>
internal readonly record struct PipeStreamReadResult(QuicStreamState State, ReadResult Result) : IQuicTransportEvent;

internal readonly record struct PipeStreamReadFailed(QuicStreamState State, Exception Error) : IQuicTransportEvent;

internal readonly record struct InboundStreamAccepted(Stream Stream, long StreamId) : IQuicTransportEvent;

internal readonly record struct MigrationDetected(EndPoint OldEndPoint, EndPoint NewEndPoint) : IQuicTransportEvent;
