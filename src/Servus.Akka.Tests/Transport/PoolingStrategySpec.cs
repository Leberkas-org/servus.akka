using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class PoolingStrategySpec
{
    [Fact(Timeout = 5000)]
    public void NoReuse_should_return_Dispose_on_disconnect()
    {
        var strategy = new NoReuseStrategy();

        Assert.Equal(PoolAction.Dispose, strategy.OnDisconnect(new object(), DisconnectReason.Graceful));
    }

    [Fact(Timeout = 5000)]
    public void NoReuse_should_return_Dispose_on_upstream_finish()
    {
        var strategy = new NoReuseStrategy();

        Assert.Equal(PoolAction.Dispose, strategy.OnUpstreamFinish(new object()));
    }

    [Fact(Timeout = 5000)]
    public void Reuse_should_return_Dispose_on_disconnect()
    {
        var strategy = new ReuseStrategy();

        Assert.Equal(PoolAction.Dispose, strategy.OnDisconnect(new object(), DisconnectReason.Error));
    }

    [Fact(Timeout = 5000)]
    public void Reuse_should_return_Reuse_on_upstream_finish()
    {
        var strategy = new ReuseStrategy();

        Assert.Equal(PoolAction.Reuse, strategy.OnUpstreamFinish(new object()));
    }

    private sealed class NoReuseStrategy : IPoolingStrategy
    {
        public PoolAction OnDisconnect(object lease, DisconnectReason reason) => PoolAction.Dispose;
        public PoolAction OnUpstreamFinish(object lease) => PoolAction.Dispose;
    }

    private sealed class ReuseStrategy : IPoolingStrategy
    {
        public PoolAction OnDisconnect(object lease, DisconnectReason reason) => PoolAction.Dispose;
        public PoolAction OnUpstreamFinish(object lease) => PoolAction.Reuse;
    }
}
