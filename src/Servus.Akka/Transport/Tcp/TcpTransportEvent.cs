namespace Servus.Akka.Transport.Tcp;

internal interface ITcpTransportEvent;

internal readonly record struct LeaseAcquired(ConnectionLease Lease) : ITcpTransportEvent;

internal readonly record struct AcquisitionFailed(Exception Error) : ITcpTransportEvent;

internal sealed class ReadCompleted(WireBuffer? Buffer, int Gen) : ITcpTransportEvent
{
    public WireBuffer? Buffer { get; } = Buffer;
    public int Gen { get; } = Gen;
}

internal sealed class ReadFailed(Exception Error, int Gen) : ITcpTransportEvent
{
    public Exception Error { get; } = Error;
    public int Gen { get; } = Gen;
}

/// <summary>Raised from the connection's send loop after a drained batch is fully sent, carrying the
/// batch's total byte count.</summary>
internal sealed class SendFlushed(int Bytes, int Gen) : ITcpTransportEvent
{
    public int Bytes { get; } = Bytes;
    public int Gen { get; } = Gen;
}
