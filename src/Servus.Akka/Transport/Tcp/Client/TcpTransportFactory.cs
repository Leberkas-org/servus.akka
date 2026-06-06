using Akka;
using Akka.Actor;
using Akka.Streams.Dsl;

namespace Servus.Akka.Transport.Tcp.Client;

public sealed class TcpTransportFactory(
    IActorRef connectionManager,
    IPoolingStrategy poolingStrategy) : ITransportFactory
{
    public Flow<ITransportOutbound, ITransportInbound, NotUsed> Create()
    {
        var stage = new TcpConnectionStage(connectionManager, poolingStrategy);
        return Flow.FromGraph(stage);
    }
}
