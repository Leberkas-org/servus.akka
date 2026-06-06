using System.Net;
using System.Net.Sockets;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class SocketAwaitableSpec : IAsyncLifetime
{
    private Socket _listener = null!;
    private Socket _client = null!;
    private Socket _server = null!;

    public async ValueTask InitializeAsync()
    {
        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        _listener.Listen(1);

        var endpoint = (IPEndPoint)_listener.LocalEndPoint!;
        _client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await _client.ConnectAsync(endpoint);
        _server = await _listener.AcceptAsync();
    }

    public ValueTask DisposeAsync()
    {
        _server?.Dispose();
        _client?.Dispose();
        _listener?.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_should_receive_sent_bytes()
    {
        var awaitable = new SocketAwaitable();
        var sent = System.Text.Encoding.UTF8.GetBytes("hello");
        await _server.SendAsync(sent, SocketFlags.None);

        var buffer = new byte[1024];
        var received = await awaitable.ReceiveAsync(_client, buffer);

        Assert.Equal(sent.Length, received);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(buffer, 0, received));
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_should_return_zero_on_fin()
    {
        var awaitable = new SocketAwaitable();
        _server.Shutdown(SocketShutdown.Send);

        var buffer = new byte[1024];
        var received = await awaitable.ReceiveAsync(_client, buffer);

        Assert.Equal(0, received);
    }

    [Fact(Timeout = 5000)]
    public async Task WaitForDataAsync_should_complete_when_data_arrives()
    {
        var awaitable = new SocketAwaitable();

        var waitTask = awaitable.WaitForDataAsync(_client);
        await _server.SendAsync(System.Text.Encoding.UTF8.GetBytes("ping"), SocketFlags.None);

        var result = await waitTask;
        Assert.Equal(0, result);
    }

    [Fact(Timeout = 5000)]
    public async Task SendAsync_should_send_bytes()
    {
        var awaitable = new SocketAwaitable();
        var data = System.Text.Encoding.UTF8.GetBytes("world");

        var bytesSent = await awaitable.SendAsync(_client, data);

        Assert.Equal(data.Length, bytesSent);

        var buffer = new byte[1024];
        var received = await _server.ReceiveAsync(buffer, SocketFlags.None);
        Assert.Equal("world", System.Text.Encoding.UTF8.GetString(buffer, 0, received));
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_should_be_reusable()
    {
        var awaitable = new SocketAwaitable();
        var buffer = new byte[1024];

        await _server.SendAsync(System.Text.Encoding.UTF8.GetBytes("one"), SocketFlags.None);
        var first = await awaitable.ReceiveAsync(_client, buffer);
        Assert.Equal("one", System.Text.Encoding.UTF8.GetString(buffer, 0, first));

        await _server.SendAsync(System.Text.Encoding.UTF8.GetBytes("two"), SocketFlags.None);
        var second = await awaitable.ReceiveAsync(_client, buffer);
        Assert.Equal("two", System.Text.Encoding.UTF8.GetString(buffer, 0, second));
    }
}
