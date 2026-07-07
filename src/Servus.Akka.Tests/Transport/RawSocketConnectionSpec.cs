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
    public async Task TryEnqueue_single_large_buffer_short_sent_should_arrive_complete_and_in_order()
    {
        // Single-buffer path (SendSingleAsync): cap each socket send well below the buffer length so the
        // partial-send remainder loop must iterate. The cap makes this deterministic, not timing-dependent.
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions())
        {
            MaxBytesPerSendForTest = 7000,
        };

        var flushed = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.OnFlushed = total => flushed.TrySetResult(total);

        var payload = new byte[100 * 1024];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i % 251);
        }

        Assert.True(connection.TryEnqueue(MakeBuffer(payload)));

        var received = await ReceiveExactlyFromServerAsync(payload.Length, TestContext.Current.CancellationToken);
        Assert.Equal(payload, received);

        var flushedTotal = await flushed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(payload.Length, flushedTotal);
    }

    [Fact(Timeout = 5000)]
    public async Task TryEnqueue_many_short_sent_mid_buffer_should_arrive_complete_and_in_order()
    {
        // Vectored path with the send capped mid-buffer (1500 does not align to the 1024-byte segments),
        // exercising the (advance < available) branch of the remainder cursor.
        await VerifyVectoredDelivery(bufferSize: 1024, bufferCount: 10, maxBytesPerSend: 1500);
    }

    [Fact(Timeout = 5000)]
    public async Task TryEnqueue_many_short_sent_at_buffer_boundary_should_arrive_complete_and_in_order()
    {
        // Vectored path with the send capped at an exact multiple of the segment size, exercising the
        // (advance == available) boundary branch of the remainder cursor.
        await VerifyVectoredDelivery(bufferSize: 1024, bufferCount: 10, maxBytesPerSend: 2 * 1024);
    }

    [Fact(Timeout = 5000)]
    public async Task TryEnqueue_many_with_paused_start_should_coalesce_into_single_vectored_send()
    {
        // Deterministic vectored coverage: park the send loop, enqueue every buffer, then release. All
        // buffers are guaranteed in the channel before the first drain, so they coalesce into one batch.
        await VerifyVectoredDelivery(bufferSize: 1024, bufferCount: 10, maxBytesPerSend: null);
    }

    private async Task VerifyVectoredDelivery(int bufferSize, int bufferCount, int? maxBytesPerSend)
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions(), gate.Task)
        {
            MaxBytesPerSendForTest = maxBytesPerSend,
        };

        var total = bufferSize * bufferCount;
        var totalFlushed = 0;
        var allFlushed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.OnFlushed = sent =>
        {
            if (Interlocked.Add(ref totalFlushed, sent) >= total)
            {
                allFlushed.TrySetResult();
            }
        };

        var expected = new byte[total];
        for (var i = 0; i < bufferCount; i++)
        {
            var chunk = new byte[bufferSize];
            for (var j = 0; j < chunk.Length; j++)
            {
                chunk[j] = (byte)((i * bufferSize + j) % 251);
            }

            Array.Copy(chunk, 0, expected, i * bufferSize, bufferSize);
            Assert.True(connection.TryEnqueue(MakeBuffer(chunk)));
        }

        // Release the parked send loop only after every buffer is queued so the drain coalesces them.
        gate.SetResult();

        var received = await ReceiveExactlyFromServerAsync(expected.Length, TestContext.Current.CancellationToken);
        Assert.Equal(expected, received);

        await allFlushed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(total, Volatile.Read(ref totalFlushed));
        Assert.True(connection.VectoredSendCount >= 1,
            $"Expected at least one vectored send, saw {connection.VectoredSendCount}.");
    }

    [Fact(Timeout = 5000)]
    public async Task OnFlushed_that_throws_should_not_fault_the_send_loop()
    {
        // A faulting consumer callback must be swallowed so subsequent sends still complete.
        await using var connection = new RawSocketConnection(_client, new TransportConnectionOptions());

        var secondFlushed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var invocations = 0;
        connection.OnFlushed = _ =>
        {
            if (Interlocked.Increment(ref invocations) == 1)
            {
                throw new InvalidOperationException("boom");
            }

            secondFlushed.TrySetResult();
        };

        Assert.True(connection.TryEnqueue(MakeBuffer("first"u8.ToArray())));
        await ReceiveExactlyFromServerAsync(5, TestContext.Current.CancellationToken);

        Assert.True(connection.TryEnqueue(MakeBuffer("second"u8.ToArray())));
        await ReceiveExactlyFromServerAsync(6, TestContext.Current.CancellationToken);

        await secondFlushed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
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
    public async Task DisposeAsync_immediately_after_create_should_not_throw()
    {
        // RawSocketConnection's send loop is started via Task.Run without passing the
        // CancellationToken to Task.Run itself (see the comment above _sendLoop's assignment) —
        // disposing immediately after construction exercises the scheduling window where the
        // loop hasn't started running yet when cancellation/teardown fires. If the token were
        // passed to Task.Run and lost the race, RunSendLoopAsync's body (and its leftover-drain
        // finally) never runs at all, so anything enqueued before DisposeAsync leaks — observable
        // via WireBuffer.Capacity staying non-zero instead of collapsing to 0 on dispose.
        for (var i = 0; i < 20; i++)
        {
            using var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            var endpoint = (IPEndPoint)_listener.LocalEndPoint!;
            await client.ConnectAsync(endpoint, TestContext.Current.CancellationToken);
            using var server = await _listener.AcceptAsync(TestContext.Current.CancellationToken);

            var connection = new RawSocketConnection(client, new TransportConnectionOptions());

            var buffer = MakeBuffer("leak-check"u8.ToArray());
            Assert.True(connection.TryEnqueue(buffer));

            await connection.DisposeAsync();

            Assert.Equal(0, buffer.Capacity);
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
