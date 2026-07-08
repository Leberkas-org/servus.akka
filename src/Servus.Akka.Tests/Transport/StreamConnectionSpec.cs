using System.Net;
using System.Net.Sockets;
using Servus.Akka.Transport;
using Servus.Diagnostics;
using static Servus.Senf;

namespace Servus.Akka.Tests.Transport;

public sealed class StreamConnectionSpec : IAsyncLifetime
{
    private Socket _listener = null!;
    private Socket _clientSocket = null!;
    private Socket _serverSocket = null!;
    private NetworkStream _clientStream = null!;
    private NetworkStream _serverStream = null!;

    public async ValueTask InitializeAsync()
    {
        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        _listener.Listen(1);

        var endpoint = (IPEndPoint)_listener.LocalEndPoint!;
        _clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await _clientSocket.ConnectAsync(endpoint, TestContext.Current.CancellationToken);
        _serverSocket = await _listener.AcceptAsync(TestContext.Current.CancellationToken);

        _clientStream = new NetworkStream(_clientSocket, ownsSocket: true);
        _serverStream = new NetworkStream(_serverSocket, ownsSocket: true);
    }

    public ValueTask DisposeAsync()
    {
        _serverStream.Dispose();
        _clientStream.Dispose();
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
            var read = await _serverStream.ReadAsync(result.AsMemory(offset), ct);
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
        await using var connection = new StreamConnection(_clientStream, new TransportConnectionOptions());

        var sent = "hello from server"u8.ToArray();
        await _serverStream.WriteAsync(sent, TestContext.Current.CancellationToken);
        await _serverStream.FlushAsync(TestContext.Current.CancellationToken);

        var buffer = await connection.ReceiveAsync();

        Assert.NotNull(buffer);
        Assert.Equal("hello from server", System.Text.Encoding.UTF8.GetString(buffer.Span));
        buffer.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_should_return_null_on_remote_close()
    {
        await using var connection = new StreamConnection(_clientStream, new TransportConnectionOptions());

        _serverSocket.Shutdown(SocketShutdown.Send);

        var buffer = await connection.ReceiveAsync();

        Assert.Null(buffer);
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_should_throw_on_concurrent_call()
    {
        await using var connection = new StreamConnection(_clientStream, new TransportConnectionOptions());

        var firstReceive = connection.ReceiveAsync().AsTask();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await connection.ReceiveAsync());

        _serverStream.Dispose();
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
        await using var connection = new StreamConnection(_clientStream, new TransportConnectionOptions());

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
    public async Task TryEnqueue_many_small_should_coalesce_and_deliver_all_bytes()
    {
        await using var connection = new StreamConnection(_clientStream, new TransportConnectionOptions());

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
    public async Task OnFlushed_that_throws_should_not_fault_the_send_loop()
    {
        await using var connection = new StreamConnection(_clientStream, new TransportConnectionOptions());

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
    public async Task QuiesceAsync_should_cancel_pending_read_and_return_true()
    {
        await using var connection = new StreamConnection(_clientStream, new TransportConnectionOptions());

        var receiveTask = connection.ReceiveAsync().AsTask();

        // No data was ever sent — the receive parks inside Stream.ReadAsync (buffer pinned).
        var clean = await connection.QuiesceAsync();
        Assert.True(clean);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await receiveTask);
    }

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_after_clean_quiesce_should_work_again()
    {
        await using var connection = new StreamConnection(_clientStream, new TransportConnectionOptions());

        var idleReceive = connection.ReceiveAsync().AsTask();
        Assert.True(await connection.QuiesceAsync());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await idleReceive);

        var sent = "after quiesce"u8.ToArray();
        await _serverStream.WriteAsync(sent, TestContext.Current.CancellationToken);
        await _serverStream.FlushAsync(TestContext.Current.CancellationToken);

        var buffer = await connection.ReceiveAsync();
        Assert.NotNull(buffer);
        Assert.Equal("after quiesce", System.Text.Encoding.UTF8.GetString(buffer.Span));
        buffer.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task QuiesceAsync_should_return_false_when_data_races_in()
    {
        var gated = new GatedReadStream();
        await using var connection = new StreamConnection(gated, new TransportConnectionOptions());

        var receiveTask = connection.ReceiveAsync().AsTask();
        await gated.WaitForReadStartedAsync();

        // QuiesceAsync cancels the receive CTS, then awaits the settle source because
        // _receiveActive is still true (ReadAsync is blocked on our gate).
        var quiesceTask = connection.QuiesceAsync().AsTask();

        // Complete the read with data — the gated stream ignores the CancellationToken, so
        // ReceiveDataAsync returns the buffer (no OCE). The settle source fires with
        // cancelled=false, making QuiesceAsync return false.
        gated.CompleteRead("x"u8.ToArray());

        var clean = await quiesceTask;
        Assert.False(clean);

        var buffer = await receiveTask;
        buffer?.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_with_pending_receive_should_not_hang()
    {
        var connection = new StreamConnection(_clientStream, new TransportConnectionOptions());

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
        // StreamConnection's send loop is started via Task.Run without passing the
        // CancellationToken to Task.Run itself (see the comment above _sendLoop's assignment) —
        // disposing immediately after construction exercises the scheduling window where the
        // loop hasn't started running yet when cancellation/teardown fires. If the token were
        // passed to Task.Run and lost the race, RunSendLoopAsync's body (and its leftover-drain
        // finally) never runs at all, so anything enqueued before DisposeAsync leaks — observable
        // via WireBuffer.Capacity staying non-zero instead of collapsing to 0 on dispose.
        for (var i = 0; i < 20; i++)
        {
            using var clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            var endpoint = (IPEndPoint)_listener.LocalEndPoint!;
            await clientSocket.ConnectAsync(endpoint, TestContext.Current.CancellationToken);
            using var serverSocket = await _listener.AcceptAsync(TestContext.Current.CancellationToken);
            await using var serverStream = new NetworkStream(serverSocket, ownsSocket: false);
            var clientStream = new NetworkStream(clientSocket, ownsSocket: false);

            var connection = new StreamConnection(clientStream, new TransportConnectionOptions());

            var buffer = MakeBuffer("leak-check"u8.ToArray());
            Assert.True(connection.TryEnqueue(buffer));

            await connection.DisposeAsync();

            Assert.Equal(0, buffer.Capacity);
        }
    }

    [Fact(Timeout = 5000)]
    public async Task TryEnqueue_after_CompleteAndDrainOutput_should_return_false_and_leave_ownership()
    {
        var connection = new StreamConnection(_clientStream, new TransportConnectionOptions());

        await connection.CompleteAndDrainOutputAsync();

        var buffer = MakeBuffer("late"u8.ToArray());
        Assert.False(connection.TryEnqueue(buffer));

        buffer.Dispose();

        await connection.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task TryEnqueue_should_fail_loud_when_quicAware_bounded_channel_full_while_active()
    {
        // Send loop is gated so nothing ever drains: capacity 2 fills on the first two enqueues, and the
        // third — while the writer is still open (active) — must fail loud (return false + log Error).
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new TransportConnectionOptions { OutboundChannelCapacity = 2 };
        var connection = new StreamConnection(_clientStream, options, quicAware: true, gate.Task);

        var listener = new RecordingErrorListener();
        Tracing.Configure(listener, TraceLevel.Error, category => category == "Transport");
        try
        {
            Assert.True(connection.TryEnqueue(MakeBuffer("a"u8.ToArray())));
            Assert.True(connection.TryEnqueue(MakeBuffer("b"u8.ToArray())));

            var overflow = MakeBuffer("c"u8.ToArray());
            Assert.False(connection.TryEnqueue(overflow));
            overflow.Dispose();
        }
        finally
        {
            Tracing.Disable();
        }

        Assert.Single(listener.Events);

        gate.SetResult();
        await connection.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task TryEnqueue_after_completion_on_bounded_channel_should_return_false_without_logging()
    {
        var options = new TransportConnectionOptions { OutboundChannelCapacity = 2 };
        var connection = new StreamConnection(_clientStream, options, quicAware: true);

        await connection.CompleteAndDrainOutputAsync();

        var listener = new RecordingErrorListener();
        Tracing.Configure(listener, TraceLevel.Error, category => category == "Transport");

        bool enqueued;
        var buffer = MakeBuffer("late"u8.ToArray());
        try
        {
            enqueued = connection.TryEnqueue(buffer);
        }
        finally
        {
            Tracing.Disable();
        }

        Assert.False(enqueued);
        Assert.Empty(listener.Events);

        buffer.Dispose();
        await connection.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task TryEnqueue_non_quicAware_should_stay_unbounded_even_with_small_capacity_option()
    {
        // quicAware defaults to false: OutboundChannelCapacity must be ignored (TCP/non-quic stays
        // unbounded), so three enqueues on a gated, never-draining send loop all still succeed.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new TransportConnectionOptions { OutboundChannelCapacity = 2 };
        var connection = new StreamConnection(_clientStream, options, quicAware: false, gate.Task);

        Assert.True(connection.TryEnqueue(MakeBuffer("a"u8.ToArray())));
        Assert.True(connection.TryEnqueue(MakeBuffer("b"u8.ToArray())));
        Assert.True(connection.TryEnqueue(MakeBuffer("c"u8.ToArray())));

        gate.SetResult();
        await connection.DisposeAsync();
    }

    /// <summary>
    /// Captures only Error-level events on the "Transport" category (already restricted by the
    /// <see cref="Tracing.Configure"/> filter passed at the call site) so the fail-loud log path in
    /// <see cref="DuplexConnectionBase.TryEnqueue"/> can be asserted directly instead of only by its
    /// boolean return value.
    /// </summary>
    private sealed class RecordingErrorListener : IServusTraceListener
    {
        private readonly List<string> _events = [];

        public IReadOnlyList<string> Events
        {
            get
            {
                lock (_events)
                {
                    return _events.ToArray();
                }
            }
        }

        public bool IsEnabled(TraceLevel level, string category) => true;

        public void Write(in TraceEvent evt)
        {
            lock (_events)
            {
                _events.Add(evt.FormatMessage());
            }
        }
    }

    [Fact(Timeout = 5000)]
    public async Task Send_batch_of_small_buffers_should_produce_single_coalesced_write()
    {
        // Park the send loop, enqueue 8 x 512-byte buffers (each < 4 KiB), then release: they all sit in
        // the channel before the first drain, so the batch coalesces into ONE WriteAsync + ONE FlushAsync.
        var recording = new RecordingStream();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var connection = new StreamConnection(recording, new TransportConnectionOptions(), gate.Task);

        var totalFlushed = 0;
        var allFlushed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.OnFlushed = total =>
        {
            if (Interlocked.Add(ref totalFlushed, total) >= 8 * 512)
            {
                allFlushed.TrySetResult();
            }
        };

        var expected = new byte[8 * 512];
        for (var i = 0; i < 8; i++)
        {
            var chunk = new byte[512];
            for (var j = 0; j < chunk.Length; j++)
            {
                chunk[j] = (byte)((i * 512 + j) % 251);
            }

            Array.Copy(chunk, 0, expected, i * 512, 512);
            Assert.True(connection.TryEnqueue(MakeBuffer(chunk)));
        }

        gate.SetResult();

        await allFlushed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(1, recording.WriteCount);
        Assert.Equal(1, recording.FlushCount);
        Assert.Equal(expected, recording.Written);
    }

    [Fact(Timeout = 5000)]
    public async Task Send_large_buffers_should_write_sequentially_without_coalescing()
    {
        // Two 64 KiB buffers (each >= 4 KiB) drained together must NOT coalesce: sequential WriteAsync per
        // buffer, one shared FlushAsync for the batch.
        var recording = new RecordingStream();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var connection = new StreamConnection(recording, new TransportConnectionOptions(), gate.Task);

        var totalFlushed = 0;
        var allFlushed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.OnFlushed = total =>
        {
            if (Interlocked.Add(ref totalFlushed, total) >= 2 * 64 * 1024)
            {
                allFlushed.TrySetResult();
            }
        };

        var expected = new byte[2 * 64 * 1024];
        for (var i = 0; i < 2; i++)
        {
            var chunk = new byte[64 * 1024];
            for (var j = 0; j < chunk.Length; j++)
            {
                chunk[j] = (byte)((i * 64 * 1024 + j) % 251);
            }

            Array.Copy(chunk, 0, expected, i * 64 * 1024, 64 * 1024);
            Assert.True(connection.TryEnqueue(MakeBuffer(chunk)));
        }

        gate.SetResult();

        await allFlushed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(2, recording.WriteCount);
        Assert.Equal(1, recording.FlushCount);
        Assert.Equal(expected, recording.Written);
    }

    /// <summary>
    /// Read-gated stream: blocks <see cref="ReadAsync(Memory{byte}, CancellationToken)"/> until
    /// <see cref="CompleteRead"/> is called, deliberately ignoring the <see cref="CancellationToken"/>
    /// so the caller can guarantee that a read completes with data even after the token has been
    /// cancelled. This makes the quiesce-race test fully deterministic.
    /// </summary>
    private sealed class GatedReadStream : MemoryStream
    {
        private readonly TaskCompletionSource _readStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<byte[]> _readData = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task WaitForReadStartedAsync() => _readStarted.Task;

        public void CompleteRead(byte[] data) => _readData.SetResult(data);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _readStarted.TrySetResult();
            var data = await _readData.Task;
            data.CopyTo(buffer);
            return data.Length;
        }
    }

    /// <summary>
    /// Write-recording stream: counts <see cref="WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/>
    /// and <see cref="FlushAsync"/> invocations and accumulates every written byte in order, so a test can
    /// assert the send loop's coalescing decision (one write vs. sequential writes) deterministically.
    /// </summary>
    private sealed class RecordingStream : Stream
    {
        private readonly MemoryStream _sink = new();
        private int _writeCount;
        private int _flushCount;

        public int WriteCount => Volatile.Read(ref _writeCount);
        public int FlushCount => Volatile.Read(ref _flushCount);
        public byte[] Written => _sink.ToArray();

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => _sink.Length;
        public override long Position { get => _sink.Position; set => throw new NotSupportedException(); }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _writeCount);
            _sink.Write(buffer.Span);
            return ValueTask.CompletedTask;
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _flushCount);
            return Task.CompletedTask;
        }

        public override void Flush() => Interlocked.Increment(ref _flushCount);
        public override void Write(byte[] buffer, int offset, int count) => _sink.Write(buffer, offset, count);
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
