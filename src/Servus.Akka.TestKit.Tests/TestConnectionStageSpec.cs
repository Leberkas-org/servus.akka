using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Servus.Akka.Transport;

namespace Servus.Akka.TestKit.Tests;

public sealed class TestConnectionStageSpec : global::Akka.TestKit.Xunit.TestKit
{
    private readonly IMaterializer _materializer;

    public TestConnectionStageSpec()
    {
        _materializer = Sys.Materializer();
    }

    [Fact(Timeout = 5000)]
    public async Task Stage_should_materialize_and_deliver_TransportConnected_via_AutoConnect()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        var tcs = new TaskCompletionSource<ITransportInbound>();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg => tcs.TrySetResult(msg)), _materializer);

        var result = await tcs.Task.WaitAsync(ct);
        Assert.IsType<TransportConnected>(result);
    }

    [Fact(Timeout = 5000)]
    public async Task Stage_should_capture_outbound_messages()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.Ignore<ITransportInbound>().MapMaterializedValue(_ => NotUsed.Instance), _materializer);

        var outbound = await stage.WaitForOutbound(ct);
        Assert.IsType<ConnectTransport>(outbound);
    }

    [Fact(Timeout = 5000)]
    public async Task Stage_should_deliver_PushOnce_messages()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        stage.PushOnce(new TransportData("HTTP/1.1 200 OK\r\n\r\n"u8.ToArray()));

        var results = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                results.Add(msg);
                if (results.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);
        Assert.IsType<TransportConnected>(results[0]);
        Assert.IsType<TransportData>(results[1]);
    }

    [Fact(Timeout = 5000)]
    public async Task Stage_should_support_bidirectional_control()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        var inboundResults = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new TransportData(new byte[] { 1, 2, 3 })
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inboundResults.Add(msg);
                if (inboundResults.Count >= 3)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        var outbound = await stage.WaitForOutbound(ct);
        Assert.IsType<ConnectTransport>(outbound);

        var dataOut = await stage.WaitForOutbound(ct);
        Assert.IsType<TransportData>(dataOut);

        stage.PushInbound(new TransportData(new byte[] { 4, 5, 6 }));
        stage.PushInbound(new TransportDisconnected(DisconnectReason.Graceful));

        await tcs.Task.WaitAsync(ct);
        Assert.IsType<TransportConnected>(inboundResults[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task Stage_should_record_activity_log()
    {
        var ct = TestContext.Current.CancellationToken;
        var log = new ActivityLog();
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .OnOutbound<ConnectTransport>((_, _) => { })
            .WithActivityLog(log)
            .Build();

        var tcs = new TaskCompletionSource<ITransportInbound>();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg => tcs.TrySetResult(msg)), _materializer);

        await tcs.Task.WaitAsync(ct);

        Assert.Contains(log.Entries, e => e is OutboundReceived);
        Assert.Contains(log.Entries, e => e is HandlerInvoked);
    }

    [Fact(Timeout = 5000)]
    public async Task Stage_should_invoke_typed_OnOutbound_handlers()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .OnOutbound<TransportData>((_, ctx) =>
            {
                ctx.Push(new TransportData(new byte[] { 0xFF }));
            })
            .Build();

        var results = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new TransportData(new byte[] { 1, 2, 3 })
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                results.Add(msg);
                if (results.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);
        Assert.IsType<TransportConnected>(results[0]);
        var responseData = Assert.IsType<TransportData>(results[1]);
        Assert.Equal(0xFF, responseData.Buffer.Span[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task Stage_should_support_implicit_flow_conversion()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        Flow<ITransportOutbound, ITransportInbound, NotUsed> flow = stage;

        var tcs = new TaskCompletionSource<ITransportInbound>();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }))
            .Via(flow)
            .RunWith(Sink.ForEach<ITransportInbound>(msg => tcs.TrySetResult(msg)), _materializer);

        var result = await tcs.Task.WaitAsync(ct);
        Assert.IsType<TransportConnected>(result);
    }

    [Fact(Timeout = 5000)]
    public async Task Stage_should_auto_respond_via_PushResponse()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        stage.PushResponse(outbound => outbound is TransportData
            ? new TransportData(new byte[] { 0xAA })
            : null);

        var results = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new TransportData(new byte[] { 1, 2, 3 })
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                results.Add(msg);
                if (results.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);
        Assert.IsType<TransportConnected>(results[0]);
        var data = Assert.IsType<TransportData>(results[1]);
        Assert.Equal(0xAA, data.Buffer.Span[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task Stage_should_support_PushResponseOnce_for_single_shot()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        stage.PushResponseOnce(_ => new TransportData(new byte[] { 0xBB }));

        var results = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new TransportData(new byte[] { 1 }),
                new TransportData(new byte[] { 2 })
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                results.Add(msg);
                if (results.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);
        Assert.IsType<TransportConnected>(results[0]);
        var data = Assert.IsType<TransportData>(results[1]);
        Assert.Equal(0xBB, data.Buffer.Span[0]);
        Assert.Equal(2, results.Count);
    }
}
