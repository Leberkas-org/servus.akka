using Akka;
using Akka.Actor;
using Akka.Streams.Dsl;
using Servus.Akka.Transport.Quic.Client;
using Servus.Akka.Transport.Quic.Listener;
using Servus.Akka.Transport.Tcp.Client;
using Servus.Akka.Transport.Tcp.Listener;

namespace Servus.Akka.Transport;

public static class TransportFactory
{
    public static Source<Flow<ITransportOutbound, ITransportInbound, NotUsed>, Task> CreateTcpListener(
        TcpListenerOptions options)
        => new TcpListenerFactory().Bind(options);

    public static Source<Flow<ITransportOutbound, ITransportInbound, NotUsed>, Task> CreateQuicListener(
        QuicListenerOptions options)
        => new QuicListenerFactory().Bind(options);

    public static Flow<ITransportOutbound, ITransportInbound, NotUsed> CreateTcpClient(IActorRef connectionManager,
        IPoolingStrategy poolingStrategy)
        => new TcpTransportFactory(connectionManager, poolingStrategy).Create();

    public static Flow<ITransportOutbound, ITransportInbound, NotUsed> CreateQuicClient(IActorRef connectionManager)
        => new QuicTransportFactory(connectionManager).Create();

    public static Props CreateTcpConnectionManager(PoolConfigRegistry registry)
        => CreateTcpConnectionManager(new TcpConnectionFactory(), registry);

    public static Props CreateQuicConnectionManager()
        => CreateQuicConnectionManager(new QuicConnectionFactory());

    internal static Props CreateTcpConnectionManager(ITcpConnectionFactory factory, PoolConfigRegistry registry)
        => Props.CreateBy(new TcpConnectionManagerProducer(factory, registry));

    internal static Props CreateQuicConnectionManager(IQuicConnectionFactory factory)
        => Props.CreateBy(new QuicConnectionManagerProducer(factory));

    private sealed class TcpConnectionManagerProducer(
        ITcpConnectionFactory factory,
        PoolConfigRegistry registry) : IIndirectActorProducer
    {
        public Type ActorType => typeof(TcpConnectionManagerActor);
        public ActorBase Produce() => new TcpConnectionManagerActor(factory, registry);
        public void Release(ActorBase actor) { }
    }

    private sealed class QuicConnectionManagerProducer(
        IQuicConnectionFactory factory) : IIndirectActorProducer
    {
        public Type ActorType => typeof(QuicConnectionManagerActor);
        public ActorBase Produce() => new QuicConnectionManagerActor(factory);
        public void Release(ActorBase actor) { }
    }
}