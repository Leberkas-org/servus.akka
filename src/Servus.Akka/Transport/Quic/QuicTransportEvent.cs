using System.Net;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Transport.Quic;

internal interface IQuicTransportEvent;

internal readonly record struct ConnectionLeaseAcquired(QuicConnectionLease Lease) : IQuicTransportEvent;

internal readonly record struct StreamLeaseAcquired(StreamHandle Handle, long StreamId) : IQuicTransportEvent;

internal readonly record struct AcquisitionFailed(Exception Error) : IQuicTransportEvent;

internal readonly record struct InboundData(TransportBuffer Buffer, long StreamId, int Gen) : IQuicTransportEvent;

internal readonly record struct InboundStreamAccepted(Stream Stream, long StreamId) : IQuicTransportEvent;

internal readonly record struct InboundComplete(DisconnectReason Reason, int Gen, long StreamId) : IQuicTransportEvent;

internal readonly record struct InboundPumpFailed(Exception Error, long StreamId) : IQuicTransportEvent;

internal readonly record struct OutboundWriteDone(long StreamId) : IQuicTransportEvent;

internal readonly record struct OutboundWriteFailed(Exception Error, long StreamId) : IQuicTransportEvent;

internal readonly record struct MigrationDetected(EndPoint OldEndPoint, EndPoint NewEndPoint) : IQuicTransportEvent;


