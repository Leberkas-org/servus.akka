using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;

namespace Servus.Akka.Transport.Tcp.Client;

public sealed class TcpTransportFactory(
    IActorRef connectionManager,
    IPoolingStrategy poolingStrategy,
    bool usePipeTransport = false) : ITransportFactory
{
    public Flow<ITransportOutbound, ITransportInbound, NotUsed> Create()
    {
        GraphStage<FlowShape<ITransportOutbound, ITransportInbound>> stage = usePipeTransport
            ? new PipeTransportStage(connectionManager, poolingStrategy)
            : new TcpConnectionStage(connectionManager, poolingStrategy);

        return Flow.FromGraph(stage);
    }
}
