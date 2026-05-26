using Akka;
using Akka.Actor;
using Akka.Streams.Dsl;

namespace Servus.Akka.Transport.Tcp.Client;

public sealed class TcpTransportFactory : ITransportFactory
{
    private readonly IActorRef _connectionManager;
    private readonly IPoolingStrategy _poolingStrategy;

    public TcpTransportFactory(IActorRef connectionManager, IPoolingStrategy poolingStrategy)
    {
        _connectionManager = connectionManager;
        _poolingStrategy = poolingStrategy;
    }

    public Flow<ITransportOutbound, ITransportInbound, NotUsed> Create()
    {
        return Flow.FromGraph(new TcpConnectionStage(_connectionManager, _poolingStrategy));
    }
}
