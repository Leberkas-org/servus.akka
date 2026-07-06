namespace Servus.Akka.Transport.Tcp;

internal interface ITcpTransportEvent;

internal readonly record struct LeaseAcquired(ConnectionLease Lease) : ITcpTransportEvent;

internal readonly record struct AcquisitionFailed(Exception Error) : ITcpTransportEvent;

internal readonly record struct ReadCompleted(WireBuffer? Buffer, int Gen) : ITcpTransportEvent;

internal readonly record struct ReadFailed(Exception Error, int Gen) : ITcpTransportEvent;

/// <summary>Raised from the connection's send loop after a drained batch is fully sent, carrying the
/// batch's total byte count. Replaces the client SM's use of <see cref="PipeFlushComplete"/>.</summary>
internal readonly record struct SendFlushed(int Bytes, int Gen) : ITcpTransportEvent;

// deleted in server-SM migration: the client SM no longer uses this; the TCP server state machine
// still relies on it until its own migration to IDuplexConnection.
internal readonly record struct PipeFlushComplete(int Gen) : ITcpTransportEvent;
