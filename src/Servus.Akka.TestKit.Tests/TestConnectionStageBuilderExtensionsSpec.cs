using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Servus.Akka.Transport;

namespace Servus.Akka.TestKit.Tests;

public sealed class TestConnectionStageBuilderExtensionsSpec : global::Akka.TestKit.Xunit.TestKit
{
    private readonly IMaterializer _materializer;

    public TestConnectionStageBuilderExtensionsSpec()
    {
        _materializer = Sys.Materializer();
    }

    [Fact(Timeout = 5000)]
    public async Task OnData_should_invoke_handler_on_TransportData()
    {
        var ct = TestContext.Current.CancellationToken;
        var handlerInvoked = false;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .OnData((_, ctx) =>
            {
                handlerInvoked = true;
                ctx.Push(new TransportData(new byte[] { 0xFF }));
            })
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new TransportData(new byte[] { 0xAA })
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);

        Assert.True(handlerInvoked, "OnData handler should have been invoked");
        Assert.IsType<TransportConnected>(inbound[0]);
        var response = Assert.IsType<TransportData>(inbound[1]);
        Assert.Equal(0xFF, response.Buffer.Span[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task OnOpenStream_should_invoke_handler_on_OpenStream()
    {
        var ct = TestContext.Current.CancellationToken;
        var handlerInvoked = false;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .OnOpenStream((open, ctx) =>
            {
                handlerInvoked = true;
                ctx.Push(new StreamOpened(open.StreamId, open.Direction));
            })
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new OpenStream(42, StreamDirection.Bidirectional)
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);

        Assert.True(handlerInvoked, "OnOpenStream handler should have been invoked");
        Assert.IsType<TransportConnected>(inbound[0]);
        var opened = Assert.IsType<StreamOpened>(inbound[1]);
        Assert.Equal(42L, opened.Id.Value);
    }

    [Fact(Timeout = 5000)]
    public async Task OnMultiplexedData_should_invoke_handler_on_MultiplexedData()
    {
        var ct = TestContext.Current.CancellationToken;
        var handlerInvoked = new TaskCompletionSource();

        var buf = TransportBuffer.Rent(2);
        buf.FullMemory.Span[0] = 0xAA;
        buf.FullMemory.Span[1] = 0xBB;
        buf.Length = 2;

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .OnMultiplexedData((_, _) =>
            {
                handlerInvoked.TrySetResult();
            })
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new MultiplexedData(buf, 7)
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.Ignore<ITransportInbound>().MapMaterializedValue(_ => NotUsed.Instance), _materializer);

        await handlerInvoked.Task.WaitAsync(ct);
    }

    [Fact(Timeout = 5000)]
    public async Task OnDisconnect_should_invoke_handler_on_DisconnectTransport()
    {
        var ct = TestContext.Current.CancellationToken;
        var handlerInvoked = false;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .OnDisconnect((disconnect, ctx) =>
            {
                handlerInvoked = true;
                ctx.Push(new TransportDisconnected(disconnect.Reason));
            })
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new DisconnectTransport(DisconnectReason.Timeout)
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);

        Assert.True(handlerInvoked, "OnDisconnect handler should have been invoked");
        Assert.IsType<TransportConnected>(inbound[0]);
        var disconnected = Assert.IsType<TransportDisconnected>(inbound[1]);
        Assert.Equal(DisconnectReason.Timeout, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public async Task AutoStreamOpened_should_respond_with_StreamOpened_for_matching_streamId()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .AutoStreamOpened(42)
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new OpenStream(42, StreamDirection.Bidirectional)
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var opened = Assert.IsType<StreamOpened>(inbound[1]);
        Assert.Equal(42L, (long)opened.Id);
        Assert.Equal(StreamDirection.Bidirectional, opened.Direction);
    }

    [Fact(Timeout = 5000)]
    public async Task AutoStreamOpened_should_not_respond_for_different_streamId()
    {
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();
        var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .AutoStreamOpened(42)
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new OpenStream(99, StreamDirection.Bidirectional)
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        // Wait for either the timeout or a second message (which shouldn't come)
        try
        {
            await tcs.Task.WaitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected: timeout after waiting for a second message that won't arrive
        }

        // Should only have TransportConnected, no StreamOpened response
        Assert.Single(inbound);
        Assert.IsType<TransportConnected>(inbound[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task EchoMultiplexedData_should_echo_back_data()
    {
        var ct = TestContext.Current.CancellationToken;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .EchoMultiplexedData()
            .Build();

        var originalData = new byte[] { 0x11, 0x22, 0x33 };
        var originalBuf = TransportBuffer.Rent(originalData.Length);
        originalData.CopyTo(originalBuf.FullMemory.Span);
        originalBuf.Length = originalData.Length;

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new MultiplexedData(originalBuf, 7)
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);

        Assert.IsType<TransportConnected>(inbound[0]);
        var echo = Assert.IsType<MultiplexedData>(inbound[1]);
        Assert.Equal(7L, (long)echo.StreamId);
        Assert.Equal(3, echo.Buffer.Length);
        Assert.Equal(0x11, echo.Buffer.Span[0]);
        Assert.Equal(0x22, echo.Buffer.Span[1]);
        Assert.Equal(0x33, echo.Buffer.Span[2]);
    }

    [Fact(Timeout = 5000)]
    public async Task OnCompleteWrites_should_invoke_handler_on_CompleteWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        var handlerInvoked = false;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .OnCompleteWrites((_, ctx) =>
            {
                handlerInvoked = true;
                ctx.Push(new TransportDisconnected(DisconnectReason.Graceful));
            })
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new CompleteWrites(0)
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);

        Assert.True(handlerInvoked, "OnCompleteWrites handler should have been invoked");
        Assert.IsType<TransportConnected>(inbound[0]);
        var disconnected = Assert.IsType<TransportDisconnected>(inbound[1]);
        Assert.Equal(DisconnectReason.Graceful, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public async Task OnResetStream_should_invoke_handler_on_ResetStream()
    {
        var ct = TestContext.Current.CancellationToken;
        var handlerInvoked = false;
        var inbound = new List<ITransportInbound>();
        var tcs = new TaskCompletionSource();

        var stage = new TestConnectionStageBuilder()
            .AutoConnect()
            .OnResetStream((reset, ctx) =>
            {
                handlerInvoked = true;
                ctx.Push(new StreamClosed(reset.StreamId, DisconnectReason.Error));
            })
            .Build();

        _ = Source.From<ITransportOutbound>([
                new ConnectTransport(new TcpTransportOptions { Host = "localhost", Port = 80 }),
                new ResetStream(99)
            ])
            .Via(stage.AsFlow())
            .RunWith(Sink.ForEach<ITransportInbound>(msg =>
            {
                inbound.Add(msg);
                if (inbound.Count >= 2)
                {
                    tcs.TrySetResult();
                }
            }), _materializer);

        await tcs.Task.WaitAsync(ct);

        Assert.True(handlerInvoked, "OnResetStream handler should have been invoked");
        Assert.IsType<TransportConnected>(inbound[0]);
        var closed = Assert.IsType<StreamClosed>(inbound[1]);
        Assert.Equal(99L, (long)closed.Id);
        Assert.Equal(DisconnectReason.Error, closed.Reason);
    }
}
