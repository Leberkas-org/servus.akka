namespace Servus.Akka.Transport;

public enum DisconnectReason
{
    Graceful,
    Timeout,
    Error,
    Evicted,
    Transient
}
