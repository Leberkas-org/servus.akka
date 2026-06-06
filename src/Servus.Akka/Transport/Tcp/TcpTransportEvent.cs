using System.IO.Pipelines;

namespace Servus.Akka.Transport.Tcp;

internal interface ITcpTransportEvent;

internal readonly record struct LeaseAcquired(ConnectionLease Lease) : ITcpTransportEvent;

internal readonly record struct AcquisitionFailed(Exception Error) : ITcpTransportEvent;

internal readonly record struct PipeReadComplete(ReadResult Result, int Gen) : ITcpTransportEvent;

internal readonly record struct PipeReadFailed(Exception Error, int Gen) : ITcpTransportEvent;
