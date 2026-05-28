using Akka;
using Akka.Actor;
using Akka.Streams.Dsl;

namespace Servus.Akka.Transport.Tcp.Client;

public sealed class TcpTransportFactory(IActorRef connectionManager, IPoolingStrategy poolingStrategy)
    : ITransportFactory
{
    public Flow<ITransportOutbound, ITransportInbound, NotUsed> Create()
    {
        return Flow.FromGraph(new TcpConnectionStage(connectionManager, poolingStrategy));
    }
}
