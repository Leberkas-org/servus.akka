using System.Net;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;

namespace Servus.Akka.Tests.Transport.Quic;

public sealed class QuicConnectionHandleSpec
{
    [Fact(Timeout = 5000)]
    public async Task OpenStreamAsync_should_delegate_to_factory()
    {
        var openStreamCalled = false;
        const long expectedStreamId = 42L;
        Stream expectedStream = new MemoryStream([0x01, 0x02, 0x03]);

        var handle = new QuicConnectionHandle(
            openStream: (dir, _) =>
            {
                openStreamCalled = true;
                Assert.Equal(StreamDirection.Bidirectional, dir);
                return Task.FromResult((expectedStream, expectedStreamId));
            },
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

        var result = await handle.OpenStreamAsync(StreamDirection.Bidirectional, TestContext.Current.CancellationToken);

        Assert.True(openStreamCalled);
        Assert.Equal(expectedStreamId, result.StreamId);
        Assert.Same(expectedStream, result.Stream);
    }

    [Fact(Timeout = 5000)]
    public async Task OpenStreamAsync_should_pass_direction_correctly()
    {
        var capturedDirections = new List<StreamDirection>();
        var handle = new QuicConnectionHandle(
            openStream: (dir, _) =>
            {
                capturedDirections.Add(dir);
                return Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L));
            },
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

        await handle.OpenStreamAsync(StreamDirection.Bidirectional, TestContext.Current.CancellationToken);
        await handle.OpenStreamAsync(StreamDirection.Unidirectional, TestContext.Current.CancellationToken);

        Assert.Equal(2, capturedDirections.Count);
        Assert.Equal(StreamDirection.Bidirectional, capturedDirections[0]);
        Assert.Equal(StreamDirection.Unidirectional, capturedDirections[1]);
    }

    [Fact(Timeout = 5000)]
    public async Task OpenStreamAsync_should_pass_cancellation_token()
    {
        var capturedTokens = new List<CancellationToken>();
        var cts = new CancellationTokenSource();

        var handle = new QuicConnectionHandle(
            openStream: (_, ct) =>
            {
                capturedTokens.Add(ct);
                return Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L));
            },
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

        await handle.OpenStreamAsync(StreamDirection.Bidirectional, cts.Token);

        Assert.Single(capturedTokens);
        Assert.Equal(cts.Token, capturedTokens[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task AcceptInboundStreamAsync_should_return_null_when_no_streams()
    {
        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

        var result = await handle.AcceptInboundStreamAsync(TestContext.Current.CancellationToken);

        Assert.Null(result);
    }

    [Fact(Timeout = 5000)]
    public async Task AcceptInboundStreamAsync_should_return_stream_when_available()
    {
        var expectedStreamId = 123L;
        var expectedStream = new MemoryStream([0xAA, 0xBB, 0xCC]);

        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(
                (expectedStream, expectedStreamId)),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

        var result = await handle.AcceptInboundStreamAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal(expectedStreamId, result.Value.StreamId);
        Assert.Same(expectedStream, result.Value.Stream);
    }

    [Fact(Timeout = 5000)]
    public async Task AcceptInboundStreamAsync_should_pass_cancellation_token()
    {
        var capturedTokens = new List<CancellationToken>();
        var cts = new CancellationTokenSource();

        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: ct =>
            {
                capturedTokens.Add(ct);
                return Task.FromResult<(Stream, long)?>(null);
            },
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

        await handle.AcceptInboundStreamAsync(cts.Token);

        Assert.Single(capturedTokens);
        Assert.Equal(cts.Token, capturedTokens[0]);
    }

    [Fact(Timeout = 5000)]
    public void LocalEndPoint_should_delegate_to_factory()
    {
        var endPoint = new IPEndPoint(IPAddress.Loopback, 8080);
        var getLocalEndPointCalled = false;

        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () =>
            {
                getLocalEndPointCalled = true;
                return endPoint;
            },
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

        var result = handle.LocalEndPoint();

        Assert.True(getLocalEndPointCalled);
        Assert.Same(endPoint, result);
    }

    [Fact(Timeout = 5000)]
    public void LocalEndPoint_should_return_null_when_unavailable()
    {
        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

        var result = handle.LocalEndPoint();

        Assert.Null(result);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_should_delegate_to_factory()
    {
        var disposeCalled = false;

        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () =>
            {
                disposeCalled = true;
                return ValueTask.CompletedTask;
            });

        Assert.False(disposeCalled);

        await handle.DisposeAsync();

        Assert.True(disposeCalled);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_should_complete_successfully()
    {
        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

        // Should not throw
        await handle.DisposeAsync();
    }
}