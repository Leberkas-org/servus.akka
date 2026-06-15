using System.Buffers;
using System.Net;
using System.Net.Sockets;
using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

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
        _server.Dispose();
        _client.Dispose();
        _listener.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_should_receive_sent_bytes()
    {
        var awaitable = new SocketAwaitable();
        var sent = "hello"u8.ToArray();
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
        await _server.SendAsync("ping"u8.ToArray(), SocketFlags.None);

        var result = await waitTask;
        Assert.Equal(0, result);
    }

    [Fact(Timeout = 5000)]
    public async Task SendAsync_should_send_bytes()
    {
        var awaitable = new SocketAwaitable();
        var data = "world"u8.ToArray();

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

        await _server.SendAsync("one"u8.ToArray(), SocketFlags.None);
        var first = await awaitable.ReceiveAsync(_client, buffer);
        Assert.Equal("one", System.Text.Encoding.UTF8.GetString(buffer, 0, first));

        await _server.SendAsync("two"u8.ToArray(), SocketFlags.None);
        var second = await awaitable.ReceiveAsync(_client, buffer);
        Assert.Equal("two", System.Text.Encoding.UTF8.GetString(buffer, 0, second));
    }

    [Fact(Timeout = 5000)]
    public async Task SendAsync_should_send_multi_segment_sequence_in_order()
    {
        var awaitable = new SocketAwaitable();
        var sequence = CreateMultiSegment("foo"u8.ToArray(), "bar"u8.ToArray(), "baz"u8.ToArray());

        var bytesSent = await awaitable.SendAsync(_client, sequence);

        Assert.Equal(9, bytesSent);

        var buffer = new byte[1024];
        var total = await ReadExactlyAsync(_server, buffer, 9);
        Assert.Equal("foobarbaz", System.Text.Encoding.UTF8.GetString(buffer, 0, total));
    }

    [Fact(Timeout = 5000)]
    public async Task SendAsync_should_be_reusable_across_single_and_multi_segment()
    {
        var awaitable = new SocketAwaitable();
        var buffer = new byte[1024];

        // Single-buffer send.
        await awaitable.SendAsync(_client, "one"u8.ToArray());
        var n1 = await ReadExactlyAsync(_server, buffer, 3);
        Assert.Equal("one", System.Text.Encoding.UTF8.GetString(buffer, 0, n1));

        // Switch to gather send (must clear the single Buffer before setting BufferList).
        await awaitable.SendAsync(_client, CreateMultiSegment("tw"u8.ToArray(), "oo"u8.ToArray()));
        var n2 = await ReadExactlyAsync(_server, buffer, 4);
        Assert.Equal("twoo", System.Text.Encoding.UTF8.GetString(buffer, 0, n2));

        // Switch back to single (must clear the BufferList before setting Buffer).
        await awaitable.SendAsync(_client, "end"u8.ToArray());
        var n3 = await ReadExactlyAsync(_server, buffer, 3);
        Assert.Equal("end", System.Text.Encoding.UTF8.GetString(buffer, 0, n3));
    }

    [Fact(Timeout = 5000)]
    public async Task SendAsync_should_fall_through_to_single_buffer_for_one_segment_sequence()
    {
        var awaitable = new SocketAwaitable();
        var sequence = new ReadOnlySequence<byte>("solo"u8.ToArray());

        var bytesSent = await awaitable.SendAsync(_client, sequence);

        Assert.Equal(4, bytesSent);

        var buffer = new byte[1024];
        var total = await ReadExactlyAsync(_server, buffer, 4);
        Assert.Equal("solo", System.Text.Encoding.UTF8.GetString(buffer, 0, total));
    }

    private static async Task<int> ReadExactlyAsync(Socket socket, byte[] buffer, int count)
    {
        var total = 0;
        while (total < count)
        {
            var read = await socket.ReceiveAsync(buffer.AsMemory(total), SocketFlags.None);
            if (read == 0)
            {
                break;
            }

            total += read;
        }

        return total;
    }

    private static ReadOnlySequence<byte> CreateMultiSegment(params byte[][] chunks)
    {
        Segment? first = null;
        Segment? last = null;

        foreach (var chunk in chunks)
        {
            if (first is null)
            {
                first = new Segment(chunk);
                last = first;
            }
            else
            {
                last = last!.Append(chunk);
            }
        }

        return new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }
}
