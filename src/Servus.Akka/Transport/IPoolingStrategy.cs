namespace Servus.Akka.Transport;

public interface IPoolingStrategy
{
    PoolAction OnDisconnect(object lease, DisconnectReason reason);
    PoolAction OnUpstreamFinish(object lease);
}
