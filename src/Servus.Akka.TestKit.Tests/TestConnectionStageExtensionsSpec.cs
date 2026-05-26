using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Servus.Akka.Transport;

namespace Servus.Akka.TestKit.Tests;

public sealed class TestConnectionStageExtensionsSpec : global::Akka.TestKit.Xunit.TestKit
{
    private readonly IMaterializer _materializer;

    public TestConnectionStageExtensionsSpec()
    {
        _materializer = Sys.Materializer();
    }

    [Fact(Timeout = 5000)]
    public async Task PushData_bytes_should_deliver_TransportData_inbound()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        stage.PushData([1, 2, 3]);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var data = Assert.IsType<TransportData>(inbound[1]);
        Assert.Equal(3, data.Buffer.Length);
    }

    [Fact(Timeout = 5000)]
    public async Task PushData_string_should_deliver_TransportData_inbound()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        stage.PushData("hello");

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        Assert.IsType<TransportData>(inbound[1]);
    }

    [Fact(Timeout = 5000)]
    public async Task PushStreamOpened_should_deliver_StreamOpened_inbound()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        stage.PushStreamOpened(42);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var opened = Assert.IsType<StreamOpened>(inbound[1]);
        Assert.Equal(42L, (long)opened.Id);
        Assert.Equal(StreamDirection.Bidirectional, opened.Direction);
    }

    [Fact(Timeout = 5000)]
    public async Task PushMultiplexedData_should_deliver_MultiplexedData_inbound()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        stage.PushMultiplexedData(7, [0xAA, 0xBB]);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var mux = Assert.IsType<MultiplexedData>(inbound[1]);
        Assert.Equal(7L, (long)mux.StreamId);
        Assert.Equal(2, mux.Buffer.Length);
    }

    [Fact(Timeout = 5000)]
    public async Task SimulateInboundStream_should_push_full_lifecycle()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 5)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        stage.SimulateInboundStream(5, StreamDirection.Unidirectional, [1, 2], [3, 4]);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var accepted = Assert.IsType<ServerStreamAccepted>(inbound[1]);
        Assert.Equal(5L, (long)accepted.Id);
        Assert.Equal(StreamDirection.Unidirectional, accepted.Direction);
        Assert.IsType<MultiplexedData>(inbound[2]);
        Assert.IsType<MultiplexedData>(inbound[3]);
        Assert.IsType<StreamReadCompleted>(inbound[4]);
    }

    [Fact(Timeout = 5000)]
    public async Task PushDisconnected_should_push_TransportDisconnected()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        stage.PushDisconnected(DisconnectReason.Timeout);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var disconnected = Assert.IsType<TransportDisconnected>(inbound[1]);
        Assert.Equal(DisconnectReason.Timeout, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public async Task WaitForDataAsync_should_skip_non_data_messages()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new TransportData(new byte[] { 0xAA })
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.Ignore<ITransportInbound>().MapMaterializedValue(_ => NotUsed.Instance), _materializer);

        var data = await stage.WaitForDataAsync(ct);
        Assert.Equal(0xAA, data.Buffer.Span[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task WaitForOpenStreamAsync_should_skip_non_open_messages()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new OpenStream(1, StreamDirection.Bidirectional)
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.Ignore<ITransportInbound>().MapMaterializedValue(_ => NotUsed.Instance), _materializer);

        var open = await stage.WaitForOpenStreamAsync(ct);
        Assert.Equal(1L, (long)open.StreamId);
        Assert.Equal(StreamDirection.Bidirectional, open.Direction);
    }

    [Fact(Timeout = 5000)]
    public async Task PushStreamClosed_should_deliver_StreamClosed_inbound()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        stage.PushStreamClosed(99, DisconnectReason.Error);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var closed = Assert.IsType<StreamClosed>(inbound[1]);
        Assert.Equal(99L, (long)closed.Id);
        Assert.Equal(DisconnectReason.Error, closed.Reason);
    }

    [Fact(Timeout = 5000)]
    public async Task PushConnectionMigration_should_deliver_ConnectionMigrationDetected_inbound()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        var oldEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("192.168.1.1"), 5000);
        var newEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("192.168.1.2"), 5001);
        stage.PushConnectionMigration(oldEndPoint, newEndPoint);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var migration = Assert.IsType<ConnectionMigrationDetected>(inbound[1]);
        Assert.Equal(oldEndPoint, migration.OldEndPoint);
        Assert.Equal(newEndPoint, migration.NewEndPoint);
    }

    [Fact(Timeout = 5000)]
    public async Task WaitForMultiplexedDataAsync_should_skip_non_multiplexed_messages()
    {
        var ct = TestContext.Current.CancellationToken;
        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new OpenStream(1, StreamDirection.Bidirectional),
                new MultiplexedData(TransportBuffer.Rent(0), 1)
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.Ignore<ITransportInbound>().MapMaterializedValue(_ => NotUsed.Instance), _materializer);

        var mux = await stage.WaitForMultiplexedDataAsync(ct);
        Assert.Equal(1L, (long)mux.StreamId);
    }

    [Fact(Timeout = 5000)]
    public async Task PushStreamReadCompleted_should_deliver_StreamReadCompleted_inbound()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        stage.PushStreamReadCompleted(42);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var completed = Assert.IsType<StreamReadCompleted>(inbound[1]);
        Assert.Equal(42L, (long)completed.Id);
    }

    [Fact(Timeout = 5000)]
    public async Task PushStreamClosed_with_error_reason_should_deliver_error()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        stage.PushStreamClosed(55, DisconnectReason.Error);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var closed = Assert.IsType<StreamClosed>(inbound[1]);
        Assert.Equal(55L, (long)closed.Id);
        Assert.Equal(DisconnectReason.Error, closed.Reason);
    }

    [Fact(Timeout = 5000)]
    public async Task PushDisconnected_default_reason_should_be_graceful()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .Build();

        _ = Source.Single<ITransportOutbound>(new ConnectTransport(new TcpTransportOptions
        { Host = "localhost", Port = 80 }))
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await stage.WaitForOutbound(ct);
        stage.PushDisconnected();

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var disconnected = Assert.IsType<TransportDisconnected>(inbound[1]);
        Assert.Equal(DisconnectReason.Graceful, disconnected.Reason);
    }
}
