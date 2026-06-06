using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.TestKit.Xunit;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

public sealed class PipeTransportStageSpec : TestKit
{
    private readonly IMaterializer _materializer;
    private readonly IPoolingStrategy _poolingStrategy;

    public PipeTransportStageSpec()
    {
        _materializer = Sys.Materializer();
        _poolingStrategy = new TestPoolingStrategy();
    }

    [Fact(Timeout = 5000)]
    public void PipeTransportStage_should_create_without_error()
    {
        var stage = new PipeTransportStage(TestActor, _poolingStrategy);

        Assert.NotNull(stage);
    }

    [Fact(Timeout = 5000)]
    public void Stage_should_have_correct_shape()
    {
        var stage = new PipeTransportStage(TestActor, _poolingStrategy);

        Assert.NotNull(stage.Shape);
        Assert.Equal("PipeTransport.In", stage.Shape.Inlet.Name);
        Assert.Equal("PipeTransport.Out", stage.Shape.Outlet.Name);
    }

    [Fact(Timeout = 5000)]
    public void Stage_shape_inlet_should_accept_ITransportOutbound()
    {
        var stage = new PipeTransportStage(TestActor, _poolingStrategy);

        Assert.IsAssignableFrom<Inlet<ITransportOutbound>>(stage.Shape.Inlet);
    }

    [Fact(Timeout = 5000)]
    public void Stage_shape_outlet_should_emit_ITransportInbound()
    {
        var stage = new PipeTransportStage(TestActor, _poolingStrategy);

        Assert.IsAssignableFrom<Outlet<ITransportInbound>>(stage.Shape.Outlet);
    }

    [Fact(Timeout = 5000)]
    public void Stage_should_materialize_without_error()
    {
        var stage = new PipeTransportStage(TestActor, _poolingStrategy);
        var flow = Flow.FromGraph(stage);

        var (sourceQueue, sinkQueue) = Source
            .Queue<ITransportOutbound>(1, OverflowStrategy.Fail)
            .ViaMaterialized(flow, Keep.Left)
            .ToMaterialized(Sink.Queue<ITransportInbound>(), Keep.Both)
            .Run(_materializer);

        Assert.NotNull(sourceQueue);
        Assert.NotNull(sinkQueue);
    }
}
