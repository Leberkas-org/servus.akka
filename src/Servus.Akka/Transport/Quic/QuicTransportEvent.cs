using System.IO.Pipelines;
using System.Net;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Transport.Quic;

internal interface IQuicTransportEvent;

internal readonly record struct ConnectionLeaseAcquired(QuicConnectionLease Lease) : IQuicTransportEvent;

internal readonly record struct StreamLeaseAcquired(Stream Stream, long StreamId) : IQuicTransportEvent;

internal readonly record struct AcquisitionFailed(Exception Error) : IQuicTransportEvent;

internal readonly record struct PipeStreamReadComplete(ReadResult Result, long StreamId, int Gen) : IQuicTransportEvent;

internal readonly record struct PipeStreamReadFailed(Exception Error, long StreamId, int Gen) : IQuicTransportEvent;

internal readonly record struct InboundStreamAccepted(Stream Stream, long StreamId) : IQuicTransportEvent;

internal readonly record struct MigrationDetected(EndPoint OldEndPoint, EndPoint NewEndPoint) : IQuicTransportEvent;
