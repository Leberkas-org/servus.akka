using Akka.Actor;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

public sealed class TcpTransportFactorySpec
{
    private static readonly IPoolingStrategy TestStrategy = new TestPoolingStrategy();

    [Fact(Timeout = 5000)]
    public void TcpTransportFactory_should_accept_valid_actor_ref()
    {
        var factory = new TcpTransportFactory(ActorRefs.Nobody, TestStrategy);

        Assert.NotNull(factory);
    }

    [Fact(Timeout = 5000)]
    public void Create_should_return_non_null_flow()
    {
        var factory = new TcpTransportFactory(ActorRefs.Nobody, TestStrategy);

        var flow = factory.Create();

        Assert.NotNull(flow);
    }

    [Fact(Timeout = 5000)]
    public void Create_should_return_independent_flows()
    {
        var factory = new TcpTransportFactory(ActorRefs.Nobody, TestStrategy);

        var flow1 = factory.Create();
        var flow2 = factory.Create();

        Assert.NotSame(flow1, flow2);
    }

}
