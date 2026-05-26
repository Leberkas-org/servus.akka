using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Utils;

internal sealed class TestPoolingStrategy : IPoolingStrategy
{
    public PoolAction OnDisconnect(object lease, DisconnectReason reason) => PoolAction.Dispose;
    public PoolAction OnUpstreamFinish(object lease) => PoolAction.Reuse;
}

internal sealed class ReusablePoolingStrategy : IPoolingStrategy
{
    public PoolAction OnDisconnect(object lease, DisconnectReason reason) => PoolAction.Reuse;
    public PoolAction OnUpstreamFinish(object lease) => PoolAction.Reuse;
}
