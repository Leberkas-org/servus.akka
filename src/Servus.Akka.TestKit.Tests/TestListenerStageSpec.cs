using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Servus.Akka.Transport;

namespace Servus.Akka.TestKit.Tests;

public sealed class TestListenerStageSpec : global::Akka.TestKit.Xunit.TestKit
{
    private readonly IMaterializer _materializer;

    public TestListenerStageSpec()
    {
        _materializer = Sys.Materializer();
    }

    [Fact(Timeout = 5000)]
    public async Task Default_should_emit_AutoConnect_connections()
    {
        var ct = TestContext.Current.CancellationToken;
        var listener = new TestListenerStageBuilder().Build();

        var flows = await listener.AsSource()
            .Take(1)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        Assert.Single(flows);

        var conn = listener.GetConnection(0);
        Assert.NotNull(conn);
    }

    [Fact(Timeout = 5000)]
    public async Task WithDefaultConnection_should_configure_emitted_connections()
    {
        var ct = TestContext.Current.CancellationToken;
        var listener = new TestListenerStageBuilder()
            .WithDefaultConnection(b => b.AutoConnect())
            .Build();

        var tcs = new TaskCompletionSource<ITransportInbound>();

        var flows = await listener.AsSource()
            .Take(1)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }))
            .Via(flows[0])
            .RunWith(Sink.ForEach<ITransportInbound>(msg => tcs.TrySetResult(msg)), _materializer);

        var result = await tcs.Task.WaitAsync(ct);
        Assert.IsType<TransportConnected>(result);
    }

    [Fact(Timeout = 5000)]
    public async Task OnAccept_should_control_per_index_behavior()
    {
        var ct = TestContext.Current.CancellationToken;
        var listener = new TestListenerStageBuilder()
            .OnAccept(index => new TestConnectionStageBuilder()
                .AutoConnect()
                .Build())
            .Build();

        await listener.AsSource()
            .Take(2)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        Assert.Equal(2, listener.AcceptedConnections.Count);
    }

    [Fact(Timeout = 5000)]
    public async Task OnAccept_returning_null_should_fall_back_to_default()
    {
        var ct = TestContext.Current.CancellationToken;
        var listener = new TestListenerStageBuilder()
            .WithDefaultConnection(b => b.AutoConnect())
            .OnAccept(index => index == 0
                ? new TestConnectionStageBuilder().AutoConnect().AutoDisconnect().Build()
                : null)
            .Build();

        await listener.AsSource()
            .Take(2)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        Assert.Equal(2, listener.AcceptedConnections.Count);

        var activity0 = listener.ActivityLog.OfType<ListenerConnectionAccepted>().First(a => a.Index == 0);
        Assert.True(activity0.FromFactory);

        var activity1 = listener.ActivityLog.OfType<ListenerConnectionAccepted>().First(a => a.Index == 1);
        Assert.False(activity1.FromFactory);
    }

    [Fact(Timeout = 5000)]
    public async Task AcceptedConnections_should_track_all_emitted_connections()
    {
        var ct = TestContext.Current.CancellationToken;
        var listener = new TestListenerStageBuilder().Build();

        await listener.AsSource()
            .Take(3)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        Assert.Equal(3, listener.AcceptedConnections.Count);
        Assert.Same(listener.GetConnection(0), listener.AcceptedConnections[0]);
        Assert.Same(listener.GetConnection(1), listener.AcceptedConnections[1]);
        Assert.Same(listener.GetConnection(2), listener.AcceptedConnections[2]);
    }

    [Fact(Timeout = 5000)]
    public async Task ActivityLog_should_record_ListenerConnectionAccepted()
    {
        var ct = TestContext.Current.CancellationToken;
        var listener = new TestListenerStageBuilder()
            .OnAccept(index => new TestConnectionStageBuilder().AutoConnect().Build())
            .Build();

        await listener.AsSource()
            .Take(2)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        var accepted = listener.ActivityLog.OfType<ListenerConnectionAccepted>().ToList();
        Assert.Equal(2, accepted.Count);
        Assert.Equal(0, accepted[0].Index);
        Assert.True(accepted[0].FromFactory);
        Assert.Equal(1, accepted[1].Index);
        Assert.True(accepted[1].FromFactory);
    }

    [Fact(Timeout = 5000)]
    public async Task Activities_should_expose_flat_entry_list()
    {
        var ct = TestContext.Current.CancellationToken;
        var listener = new TestListenerStageBuilder().Build();

        await listener.AsSource()
            .Take(1)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        Assert.Single(listener.Activities);
        Assert.IsType<ListenerConnectionAccepted>(listener.Activities[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task Emitted_connection_should_be_fully_functional_TestConnectionStage()
    {
        var ct = TestContext.Current.CancellationToken;
        var listener = new TestListenerStageBuilder()
            .WithDefaultConnection(b => b.AutoConnect())
            .Build();

        var tcs = new TaskCompletionSource<ITransportInbound>();

        var connectionFlows = await listener.AsSource()
            .Take(1)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }))
            .Via(connectionFlows[0])
            .RunWith(Sink.ForEach<ITransportInbound>(msg => tcs.TrySetResult(msg)), _materializer);

        var result = await tcs.Task.WaitAsync(ct);
        Assert.IsType<TransportConnected>(result);

        var conn = listener.GetConnection(0);
        var outbound = await conn.WaitForOutbound(ct);
        Assert.IsType<ConnectTransport>(outbound);
    }

    [Fact(Timeout = 5000)]
    public void Implicit_source_conversion_should_work()
    {
        var listener = new TestListenerStageBuilder().Build();

        Source<Flow<ITransportOutbound, ITransportInbound, NotUsed>, NotUsed> source = listener;

        Assert.NotNull(source);
    }

    [Fact(Timeout = 5000)]
    public async Task OnAccept_factory_should_receive_incrementing_indices()
    {
        var ct = TestContext.Current.CancellationToken;
        var indices = new List<int>();

        var listener = new TestListenerStageBuilder()
            .OnAccept(index =>
            {
                indices.Add(index);
                return new TestConnectionStageBuilder().AutoConnect().Build();
            })
            .Build();

        await listener.AsSource()
            .Take(3)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        Assert.Equal([0, 1, 2], indices);
    }

    [Fact(Timeout = 5000)]
    public void GetConnection_out_of_range_should_throw()
    {
        var listener = new TestListenerStageBuilder().Build();

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => listener.GetConnection(0));
        Assert.NotNull(ex);
    }

    [Fact(Timeout = 5000)]
    public async Task Builder_with_no_config_should_use_AutoConnect_default()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var listener = new TestListenerStageBuilder().Build();

        var connectionFlows = await listener.AsSource()
            .Take(1)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }))
            .Via(connectionFlows[0])
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 1)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task Multiple_accepts_should_create_independent_connections()
    {
        var ct = TestContext.Current.CancellationToken;
        var listener = new TestListenerStageBuilder().Build();

        await listener.AsSource()
            .Take(2)
            .RunWith(Sink.Seq<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(), _materializer)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        var conn0 = listener.GetConnection(0);
        var conn1 = listener.GetConnection(1);

        Assert.NotSame(conn0, conn1);
    }
}
