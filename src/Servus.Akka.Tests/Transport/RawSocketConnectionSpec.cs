using System.Net;
using System.Net.Sockets;
using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class RawSocketConnectionSpec : IAsyncLifetime
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
        await _client.ConnectAsync(endpoint, TestContext.Current.CancellationToken);
        _server = await _listener.AcceptAsync(TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        _server.Dispose();
        _client.Dispose();
        _listener.Dispose();
        return ValueTask.CompletedTask;
    }

    private static WireBuffer MakeBuffer(byte[] data)
    {
        var buffer = WireBuffer.Rent(data.Length);
        data.CopyTo(buffer.FullMemory.Span);
        buffer.Length = data.Length;
        return buffer;
    }

    private async Task<byte[]> ReceiveExactlyFromServerAsync(int count, CancellationToken ct)
    {
        var result = new byte[count];
        var offset = 0;
        while (offset < count)
        {
            var read = await _server.ReceiveAsync(result.AsMemory(offset), SocketFlags.None, ct);
            if (read == 0)
            {
                throw new IOException("Peer closed before all bytes arrived.");
            }

            offset += read;
        }

        return result;
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_should_deliver_sent_bytes_in_owned_buffer()
    {
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        var sent = "hello from server"u8.ToArray();
        await _server.SendAsync(sent, SocketFlags.None, TestContext.Current.CancellationToken);

        var buffer = await connection.ReceiveAsync();

        Assert.NotNull(buffer);
        Assert.Equal("hello from server", System.Text.Encoding.UTF8.GetString(buffer.Span));
        buffer.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_should_return_null_on_remote_close()
    {
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        _server.Shutdown(SocketShutdown.Send);

        var buffer = await connection.ReceiveAsync();

        Assert.Null(buffer);
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_should_throw_on_concurrent_call()
    {
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        var firstReceive = connection.ReceiveAsync().AsTask();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await connection.ReceiveAsync());

        _server.Dispose();
        try
        {
            var buffer = await firstReceive;
            buffer?.Dispose();
        }
        catch (Exception ex) when (ConnectionErrors.IsTeardown(ex))
        {
        }
    }

    [Fact(Timeout = 5000)]
    public async Task TryEnqueue_should_send_buffer_and_invoke_onFlushed_with_byte_count()
    {
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        var flushed = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.OnFlushed = total => flushed.TrySetResult(total);

        var payload = "hello from client"u8.ToArray();
        Assert.True(connection.TryEnqueue(MakeBuffer(payload)));

        var received = await ReceiveExactlyFromServerAsync(payload.Length, TestContext.Current.CancellationToken);
        Assert.Equal("hello from client", System.Text.Encoding.UTF8.GetString(received));

        var flushedTotal = await flushed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(payload.Length, flushedTotal);
    }

    [Fact(Timeout = 5000)]
    public async Task TryEnqueue_many_should_coalesce_into_vectored_send_and_deliver_all_bytes()
    {
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        var totalFlushed = 0;
        var allFlushed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.OnFlushed = total =>
        {
            if (Interlocked.Add(ref totalFlushed, total) >= 10 * 1024)
            {
                allFlushed.TrySetResult();
            }
        };

        var expected = new byte[10 * 1024];
        for (var i = 0; i < 10; i++)
        {
            var chunk = new byte[1024];
            for (var j = 0; j < chunk.Length; j++)
            {
                chunk[j] = (byte)((i * 1024 + j) % 251);
            }

            Array.Copy(chunk, 0, expected, i * 1024, 1024);
            Assert.True(connection.TryEnqueue(MakeBuffer(chunk)));
        }

        var received = await ReceiveExactlyFromServerAsync(expected.Length, TestContext.Current.CancellationToken);
        Assert.Equal(expected, received);

        await allFlushed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(10 * 1024, Volatile.Read(ref totalFlushed));
    }

    [Fact(Timeout = 5000)]
    public async Task QuiesceAsync_should_cancel_idle_probe_and_return_true()
    {
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        var receiveTask = connection.ReceiveAsync().AsTask();

        // No data was ever sent — the receive parks on the zero-byte probe.
        var clean = await connection.QuiesceAsync();
        Assert.True(clean);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await receiveTask);
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_after_clean_quiesce_should_work_again()
    {
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        var idleReceive = connection.ReceiveAsync().AsTask();
        Assert.True(await connection.QuiesceAsync());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await idleReceive);

        var sent = "after quiesce"u8.ToArray();
        await _server.SendAsync(sent, SocketFlags.None, TestContext.Current.CancellationToken);

        var buffer = await connection.ReceiveAsync();
        Assert.NotNull(buffer);
        Assert.Equal("after quiesce", System.Text.Encoding.UTF8.GetString(buffer.Span));
        buffer.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task QuiesceAsync_should_return_false_when_data_races_in()
    {
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        var clean = true;
        for (var attempt = 0; attempt < 200 && clean; attempt++)
        {
            var receiveTask = connection.ReceiveAsync().AsTask();

            // Push data so the probe can wake before quiesce cancels it.
            await _server.SendAsync("x"u8.ToArray(), SocketFlags.None, TestContext.Current.CancellationToken);

            clean = await connection.QuiesceAsync();

            // Drain whatever the receive produced so no buffer leaks, regardless of the race outcome.
            try
            {
                var buffer = await receiveTask;
                buffer?.Dispose();
            }
            catch (OperationCanceledException)
            {
            }
        }

        Assert.False(clean);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_with_pending_receive_should_not_hang()
    {
        var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        var receiveTask = connection.ReceiveAsync().AsTask();
        await connection.DisposeAsync();

        try
        {
            var buffer = await receiveTask;
            buffer?.Dispose();
        }
        catch (Exception ex)
        {
            Assert.True(ConnectionErrors.IsTeardown(ex), $"Unexpected exception type: {ex.GetType()}");
        }
    }

    [Fact(Timeout = 5000)]
    public async Task TryEnqueue_after_CompleteAndDrainOutput_should_return_false_and_leave_ownership()
    {
        var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        await connection.CompleteAndDrainOutputAsync();

        var buffer = MakeBuffer("late"u8.ToArray());
        Assert.False(connection.TryEnqueue(buffer));

        // Ownership stays with us — disposing it is our responsibility and must not double-dispose.
        buffer.Dispose();

        await connection.DisposeAsync();
    }
}
