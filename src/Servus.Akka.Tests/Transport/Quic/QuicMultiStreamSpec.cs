using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic.Client;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Quic;

public sealed class QuicMultiStreamSpec
{
    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public void TcpClientProvider_can_be_instantiated()
    {
        var provider = new TcpClientProvider(new TcpTransportOptions { Host = "localhost", Port = 80 });
        Assert.NotNull(provider);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public void TlsClientProvider_can_be_instantiated()
    {
        var provider = new TlsClientProvider(new TlsTransportOptions { Host = "localhost", Port = 443 });
        Assert.NotNull(provider);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public void QuicClientProvider_can_be_instantiated()
    {
        var provider = new QuicClientProvider(new QuicTransportOptions { Host = "example.com", Port = 443 });
        Assert.NotNull(provider);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public void DefaultInterface_SupportsMultipleStreams_ReturnsFalse()
    {
        IClientProvider provider = new MinimalClientProvider();
        Assert.False(provider.SupportsMultipleStreams);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task QuicClientProvider_ThrowsOnEmptyHost()
    {
        var provider = new QuicClientProvider(new QuicTransportOptions { Host = "", Port = 443 });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.GetStreamAsync(TestContext.Current.CancellationToken));
        Assert.Contains("SNI", ex.Message);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task QuicClientProvider_ThrowsOnNullHost()
    {
        var provider = new QuicClientProvider(new QuicTransportOptions { Host = null!, Port = 443 });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.GetStreamAsync(TestContext.Current.CancellationToken));
        Assert.Contains("SNI", ex.Message);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public void QuicClientProvider_can_be_instantiated_with_host_and_port()
    {
        var provider = new QuicClientProvider(new QuicTransportOptions { Host = "example.com", Port = 443 });
        Assert.NotNull(provider);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task ReentrantStreamProvider_OpensMultipleStreams()
    {
        var provider = new FakeReentrantProvider(streamCount: 5);

        var stream1 = await provider.GetStreamAsync(TestContext.Current.CancellationToken);
        var stream2 = await provider.GetStreamAsync(TestContext.Current.CancellationToken);
        var stream3 = await provider.GetStreamAsync(TestContext.Current.CancellationToken);

        Assert.NotSame(stream1, stream2);
        Assert.NotSame(stream2, stream3);
        Assert.Equal(1, provider.ConnectionCount);
        Assert.Equal(3, provider.StreamCount);
        Assert.True(provider.SupportsMultipleStreams);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task ConcurrentGetStreamAsync_CreatesOneConnection()
    {
        var provider = new FakeReentrantProvider(streamCount: 10, connectDelay: TimeSpan.FromMilliseconds(50));

        // Launch 5 concurrent GetStreamAsync calls
        var tasks = new Task<Stream>[5];
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = provider.GetStreamAsync(TestContext.Current.CancellationToken);
        }

        var streams = await Task.WhenAll(tasks);

        Assert.Equal(1, provider.ConnectionCount);
        Assert.Equal(5, provider.StreamCount);

        // All streams should be distinct
        for (var i = 0; i < streams.Length; i++)
        {
            for (var j = i + 1; j < streams.Length; j++)
            {
                Assert.NotSame(streams[i], streams[j]);
            }
        }
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task DeadConnection_TriggersReconnect()
    {
        var provider = new FakeReentrantProvider(streamCount: 10);

        // First stream succeeds
        var stream1 = await provider.GetStreamAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, provider.ConnectionCount);

        // Simulate connection death
        provider.KillConnection();

        // Next call should reconnect
        var stream2 = await provider.GetStreamAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, provider.ConnectionCount);
        Assert.NotSame(stream1, stream2);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task StreamOpenFailure_WrapsAsReconnectableError()
    {
        var provider = new FakeReentrantProvider(streamCount: 10, failStreamOpen: true);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.GetStreamAsync(TestContext.Current.CancellationToken));
        Assert.Contains("no longer usable", ex.Message);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task QuicClientProvider_DisposeAsync_should_be_idempotent()
    {
        var provider = new QuicClientProvider(new QuicTransportOptions { Host = "example.com", Port = 443 });

        // Should not throw on first dispose
        await provider.DisposeAsync();

        // Should not throw on second dispose
        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task QuicClientProvider_DisposeAsync_without_connection_should_complete()
    {
        var provider = new QuicClientProvider(new QuicTransportOptions { Host = "example.com", Port = 443 });

        // Dispose without ever calling GetStreamAsync (no connection established)
        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public void QuicClientProvider_LocalEndPoint_should_be_null_before_connect()
    {
        var provider = new QuicClientProvider(new QuicTransportOptions { Host = "example.com", Port = 443 });

        Assert.Null(provider.LocalEndPoint);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task QuicClientProvider_GetStreamAsync_with_empty_host_should_throw_InvalidOperationException()
    {
        var provider = new QuicClientProvider(new QuicTransportOptions { Host = "", Port = 443 });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.GetStreamAsync(TestContext.Current.CancellationToken));
        Assert.Contains("SNI", ex.Message);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task QuicClientProvider_ConcurrentDispose_should_be_safe()
    {
        var provider = new QuicClientProvider(new QuicTransportOptions { Host = "example.com", Port = 443 });

        // Launch concurrent dispose calls
        var tasks = new Task[5];
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = provider.DisposeAsync().AsTask();
        }

        // Should complete without throwing
        await Task.WhenAll(tasks);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9114-3")]
    public async Task QuicClientProvider_GetStreamAsync_should_respect_cancellation()
    {
        var provider = new QuicClientProvider(new QuicTransportOptions { Host = "example.com", Port = 443 });

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Should throw TaskCanceledException due to pre-cancelled token
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            provider.GetStreamAsync(cts.Token));
    }
}