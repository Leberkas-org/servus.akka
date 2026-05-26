using Akka.Streams;
using Servus.Akka.Transport;

namespace Servus.Akka.TestKit.Tests;

public sealed class TestPipelineSpec : global::Akka.TestKit.Xunit.TestKit
{
    private readonly IMaterializer _materializer;

    public TestPipelineSpec()
    {
        _materializer = Sys.Materializer();
    }

    [Fact(Timeout = 5000)]
    public async Task RunAsync_should_return_single_result()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        var result = await TestPipeline.RunAsync(
            stage.AsFlow(),
            new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
            _materializer, ct: ct);

        Assert.IsType<TransportConnected>(result);
    }

    [Fact(Timeout = 5000)]
    public async Task RunManyAsync_should_collect_expected_count()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .OnOutbound<TransportData>((_, ctx) =>
                ctx.Push(new TransportData(new byte[] { 0x01 })))
            .Build();

        var inputs = new ITransportOutbound[]
        {
            new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
            new TransportData(new byte[] { 1 }),
            new TransportData(new byte[] { 2 })
        };

        var results = await TestPipeline.RunManyAsync(
            stage.AsFlow(), inputs, 3, _materializer, ct: ct);

        Assert.Equal(3, results.Count);
        Assert.IsType<TransportConnected>(results[0]);
        Assert.IsType<TransportData>(results[1]);
        Assert.IsType<TransportData>(results[2]);
    }
}