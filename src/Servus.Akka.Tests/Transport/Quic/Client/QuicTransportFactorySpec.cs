using Akka.Actor;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Tests.Transport.Quic.Client;

public sealed class QuicTransportFactorySpec
{
    [Fact(Timeout = 5000)]
    public void Create_should_return_non_null_flow()
    {
        var factory = new QuicTransportFactory(ActorRefs.Nobody);

        var flow = factory.Create();

        Assert.NotNull(flow);
    }
}