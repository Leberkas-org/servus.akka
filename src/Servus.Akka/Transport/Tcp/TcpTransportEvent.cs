namespace Servus.Akka.Transport.Tcp;

internal interface ITcpTransportEvent;

internal readonly record struct LeaseAcquired(ConnectionLease Lease) : ITcpTransportEvent;

internal readonly record struct AcquisitionFailed(Exception Error) : ITcpTransportEvent;

internal readonly record struct ReadCompleted(WireBuffer? Buffer, int Gen) : ITcpTransportEvent;

internal readonly record struct ReadFailed(Exception Error, int Gen) : ITcpTransportEvent;

internal readonly record struct PipeFlushComplete(int Gen) : ITcpTransportEvent;
