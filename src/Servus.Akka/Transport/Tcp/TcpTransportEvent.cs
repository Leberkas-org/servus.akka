namespace Servus.Akka.Transport.Tcp;

internal interface ITcpTransportEvent;

internal readonly record struct LeaseAcquired(ConnectionLease Lease) : ITcpTransportEvent;

internal readonly record struct AcquisitionFailed(Exception Error) : ITcpTransportEvent;

internal readonly record struct InboundBatch(ITransportInbound[] Batch, int Count, int Gen) : ITcpTransportEvent;

internal readonly record struct InboundComplete(DisconnectReason Reason, int Gen) : ITcpTransportEvent;

internal readonly record struct InboundPumpFailed(Exception Error) : ITcpTransportEvent;

internal readonly record struct OutboundWriteDone(int Gen) : ITcpTransportEvent;

internal readonly record struct OutboundWriteFailed(Exception Error) : ITcpTransportEvent;
