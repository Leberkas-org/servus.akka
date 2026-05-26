using System.Net;
using System.Net.Sockets;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

public sealed class TcpConnectionFactorySpec : IAsyncLifetime
{
    private TcpListener? _listener;
    private int _port;

    public ValueTask InitializeAsync()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _listener?.Stop();
        return ValueTask.CompletedTask;
    }

    private TcpTransportOptions CreateOptions() => new()
    {
        Host = "127.0.0.1",
        Port = (ushort)_port
    };

    [Fact(Timeout = 5000)]
    public async Task EstablishAsync_should_return_live_lease()
    {
        var factory = new TcpConnectionFactory();
        var options = CreateOptions();

        using var lease = await factory.EstablishAsync(options, TestContext.Current.CancellationToken);

        Assert.NotNull(lease);
        Assert.True(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public async Task EstablishAsync_should_throw_on_pre_cancelled_token()
    {
        var factory = new TcpConnectionFactory();
        var options = CreateOptions();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            factory.EstablishAsync(options, cts.Token));
    }

    [Fact(Timeout = 5000)]
    public async Task EstablishAsync_should_throw_when_cancelled_during_connect()
    {
        var factory = new TcpConnectionFactory();
        var options = new TcpTransportOptions
        {
            Host = "192.0.2.1",
            Port = 80,
            ConnectTimeout = TimeSpan.FromSeconds(30)
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            factory.EstablishAsync(options, cts.Token));
    }

    [Fact(Timeout = 5000)]
    public async Task EstablishAsync_should_throw_on_connection_refused()
    {
        _listener!.Stop();

        var factory = new TcpConnectionFactory();
        var options = CreateOptions();

        await Assert.ThrowsAnyAsync<SocketException>(() =>
            factory.EstablishAsync(options, TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = 5000)]
    public async Task Disposing_lease_should_mark_it_not_alive()
    {
        var factory = new TcpConnectionFactory();
        var options = CreateOptions();

        var lease = await factory.EstablishAsync(options, TestContext.Current.CancellationToken);

        Assert.True(lease.IsAlive());

        lease.Dispose();

        Assert.False(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public async Task EstablishAsync_should_throw_on_unsupported_options()
    {
        var factory = new TcpConnectionFactory();
        var options = new QuicTransportOptions
        {
            Host = "127.0.0.1",
            Port = (ushort)_port
        };

        await Assert.ThrowsAsync<ArgumentException>(() =>
            factory.EstablishAsync(options, TestContext.Current.CancellationToken));
    }
}
