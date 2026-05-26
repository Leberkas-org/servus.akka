using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

public sealed class ClientByteMoverSpec
{
    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_complete_on_stream_read()
    {
        var stream = new MemoryStream([0x42], writable: false);
        var state = new ClientState(stream);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveStreamToChannel(state, () => { }, cts.Token);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_write_data_to_inbound_channel()
    {
        var stream = new MemoryStream([0xAB, 0xCD], writable: false);
        var state = new ClientState(stream);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveStreamToChannel(state, () => { }, cts.Token);

        Assert.True(state.InboundReader.TryRead(out var buf));
        Assert.Equal(2, buf.Length);
        Assert.Equal(0xAB, buf.Span[0]);
        Assert.Equal(0xCD, buf.Span[1]);
        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_drain_outbound_channel_to_stream()
    {
        var capturedWrites = new List<byte[]>();
        var stream = new CapturingStream(capturedWrites);
        var state = new ClientState(stream);

        WriteToChannel(state, 100, 0x11);
        WriteToChannel(state, 100, 0x22);
        WriteToChannel(state, 100, 0x33);
        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        var totalBytes = capturedWrites.Sum(w => w.Length);
        Assert.Equal(300, totalBytes);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_write_large_buffers_to_stream()
    {
        var capturedWrites = new List<byte[]>();
        var stream = new CapturingStream(capturedWrites);
        var state = new ClientState(stream);

        WriteToChannel(state, 33 * 1024, 0xAA);
        WriteToChannel(state, 100, 0xBB);
        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        var totalBytes = capturedWrites.Sum(w => w.Length);
        Assert.Equal(33 * 1024 + 100, totalBytes);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_cancellation()
    {
        var stream = new MemoryStream([0x42], writable: false);
        var state = new ClientState(stream);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await ClientByteMover.MoveStreamToChannel(state, () => { }, cts.Token);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_complete_channel_on_eof()
    {
        var stream = new MemoryStream([], writable: false);
        var state = new ClientState(stream);
        var closeCalled = false;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveStreamToChannel(state, () => closeCalled = true, cts.Token);

        Assert.True(closeCalled);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_complete_channel_with_exception_on_read_error()
    {
        var stream = new FailingStream();
        var state = new ClientState(stream);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveStreamToChannel(state, () => { }, cts.Token);

        await Assert.ThrowsAsync<AbruptCloseException>(async () =>
        {
            await state.InboundReader.WaitToReadAsync(cts.Token);
        });
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_invoke_on_writes_complete_callback()
    {
        var callbackInvoked = false;
        var stream = new MemoryStream();
        var state = new ClientState(stream)
        {
            OnWritesComplete = () => { callbackInvoked = true; }
        };

        WriteToChannel(state, 10, 0x00);
        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        Assert.True(callbackInvoked);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_drain_write_exception()
    {
        var stream = new FailingStream();
        var state = new ClientState(stream);

        WriteToChannel(state, 10, 0x00);
        state.OutboundWriter.TryComplete();

        var onCloseCalled = false;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { onCloseCalled = true; }, cts.Token);

        Assert.True(onCloseCalled);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_alternating_large_small_buffers()
    {
        var capturedWrites = new List<byte[]>();
        var stream = new CapturingStream(capturedWrites);
        var state = new ClientState(stream);

        WriteToChannel(state, 33 * 1024, 0xAA);
        WriteToChannel(state, 100, 0xBB);
        WriteToChannel(state, 33 * 1024, 0xCC);
        WriteToChannel(state, 100, 0xDD);
        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        var totalBytes = capturedWrites.Sum(w => w.Length);
        Assert.Equal(2 * (33 * 1024) + 200, totalBytes);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_not_invoke_on_writes_complete_on_error()
    {
        var callbackInvoked = false;
        var stream = new FailingStream();
        var state = new ClientState(stream)
        {
            OnWritesComplete = () => { callbackInvoked = true; }
        };

        WriteToChannel(state, 10, 0x00);
        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        Assert.False(callbackInvoked);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_not_invoke_on_writes_complete_on_cancellation()
    {
        var callbackInvoked = false;
        var stream = new SlowStream();
        var state = new ClientState(stream)
        {
            OnWritesComplete = () => { callbackInvoked = true; }
        };

        WriteToChannel(state, 10, 0x00);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        Assert.False(callbackInvoked);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_many_small_buffers()
    {
        var capturedWrites = new List<byte[]>();
        var stream = new CapturingStream(capturedWrites);
        var state = new ClientState(stream);

        for (var i = 0; i < 200; i++)
        {
            WriteToChannel(state, 100, (byte)(i % 256));
        }

        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        var totalBytes = capturedWrites.Sum(w => w.Length);
        Assert.Equal(20_000, totalBytes);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_call_on_close_exactly_once_on_read_error()
    {
        var stream = new FailingStream();
        var state = new ClientState(stream);

        var closeCount = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveStreamToChannel(state, () => Interlocked.Increment(ref closeCount), cts.Token);

        Assert.Equal(1, closeCount);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_drain_pipe_to_channel_with_abrupt_close()
    {
        var stream = new MemoryStream([0xAA, 0xBB], writable: false);
        var state = new ClientState(stream);
        var closeCount = 0;

        var ct = TestContext.Current.CancellationToken;
        var task = Task.Run(async () =>
        {
            await Task.Delay(50, ct);
            try
            {
                await state.InboundPipe.Writer.CompleteAsync(new AbruptCloseException());
            }
            catch
            {
                // noop - writer might already be completed
            }
        }, ct);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveStreamToChannel(state, () => Interlocked.Increment(ref closeCount), cts.Token);
        await task;

        Assert.Equal(1, closeCount);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_drain_pipe_to_channel_generic_exception()
    {
        var stream = new MemoryStream([0xAA, 0xBB], writable: false);
        var state = new ClientState(stream);
        var closeCount = 0;

        var ct = TestContext.Current.CancellationToken;
        var task = Task.Run(async () =>
        {
            await Task.Delay(50, ct);
            try
            {
                await state.InboundPipe.Writer.CompleteAsync(new InvalidOperationException("Test error"));
            }
            catch
            {
                // noop - writer might already be completed
            }
        }, ct);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveStreamToChannel(state, () => Interlocked.Increment(ref closeCount), cts.Token);
        await task;

        Assert.Equal(1, closeCount);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_read_final_data_after_pipe_completion()
    {
        var stream = new MemoryStream([0xAA, 0xBB, 0xCC], writable: false);
        var state = new ClientState(stream);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveStreamToChannel(state, () => { }, cts.Token);

        Assert.True(state.InboundReader.TryRead(out var buf));
        Assert.Equal(3, buf.Length);
        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_drain_pipe_to_stream_with_multi_segment_buffer()
    {
        var capturedWrites = new List<byte[]>();
        var stream = new CapturingStream(capturedWrites);
        var state = new ClientState(stream);

        WriteToChannel(state, 100, 0x11);
        WriteToChannel(state, 100, 0x22);
        WriteToChannel(state, 100, 0x33);
        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        var totalBytes = capturedWrites.Sum(w => w.Length);
        Assert.Equal(300, totalBytes);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_drain_pipe_to_stream_write_cancellation()
    {
        var stream = new SlowStream();
        var state = new ClientState(stream);
        var closeCount = 0;

        WriteToChannel(state, 100, 0x44);
        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await ClientByteMover.MoveChannelToStream(state, () => Interlocked.Increment(ref closeCount), cts.Token);

        Assert.Equal(1, closeCount);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_fill_pipe_from_channel_generic_exception()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        WriteToChannel(state, 10, 0x00);
        state.OutboundWriter.TryComplete(new InvalidOperationException("Channel error"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        Assert.True(stream.Length > 0);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_complete_channel_with_abrupt_exception_on_drain_error()
    {
        var stream = new FailingStream();
        var state = new ClientState(stream);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveStreamToChannel(state, () => { }, cts.Token);

        // Verify channel is completed with AbruptCloseException
        var exceptionThrown = false;
        try
        {
            await state.InboundReader.WaitToReadAsync(TestContext.Current.CancellationToken);
        }
        catch (AbruptCloseException)
        {
            exceptionThrown = true;
        }

        Assert.True(exceptionThrown);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_operation_cancelled_on_fill_pipe_from_stream()
    {
        var stream = new MemoryStream([0xAA, 0xBB, 0xCC, 0xDD], writable: false);
        var state = new ClientState(stream);

        using var cts = new CancellationTokenSource();
        var ct = TestContext.Current.CancellationToken;
        var fillTask = Task.Run(async () =>
        {
            await Task.Delay(100, ct);
            cts.Cancel();
        }, ct);

        await ClientByteMover.MoveStreamToChannel(state, () => { }, cts.Token);
        await fillTask;
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_io_exception_on_fill_pipe_from_stream()
    {
        var stream = new ThrowingReadStream();
        var state = new ClientState(stream);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveStreamToChannel(state, () => { }, cts.Token);

        // Should complete with AbruptCloseException on channel
        var exceptionThrown = false;
        try
        {
            await state.InboundReader.WaitToReadAsync(TestContext.Current.CancellationToken);
        }
        catch (AbruptCloseException)
        {
            exceptionThrown = true;
        }

        Assert.True(exceptionThrown);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_pipe_writer_backpressure_on_flush()
    {
        var capturedWrites = new List<byte[]>();
        var stream = new CapturingStream(capturedWrites);
        var state = new ClientState(stream);

        WriteToChannel(state, 1024 * 1024 + 100, 0xFF); // Exceed pause threshold
        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        var totalBytes = capturedWrites.Sum(w => w.Length);
        Assert.Equal(1024 * 1024 + 100, totalBytes);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_operation_cancelled_on_fill_pipe_from_channel()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        using var cts = new CancellationTokenSource();
        var ct = TestContext.Current.CancellationToken;
        var writeTask = Task.Run(async () =>
        {
            await Task.Delay(50, ct);
            cts.Cancel();
        }, ct);

        // Write data before cancellation
        var buf = TransportBuffer.Rent(100);
        buf.Length = 100;
        state.OutboundWriter.TryWrite(buf);

        // Cancel while waiting for more data
        cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        var drainTask = ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);
        await Task.WhenAll(drainTask, writeTask);
    }

    private static void WriteToChannel(ClientState state, int size, byte fill)
    {
        var buf = TransportBuffer.Rent(size);
        buf.FullMemory.Span[..size].Fill(fill);
        buf.Length = size;
        state.OutboundWriter.TryWrite(buf);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_multi_segment_buffer_with_error_on_write()
    {
        var failingStream = new FailingStream();
        var state = new ClientState(failingStream);

        // Write multiple buffers to create multi-segment potential in the pipe
        WriteToChannel(state, 100, 0x11);
        WriteToChannel(state, 100, 0x22);
        WriteToChannel(state, 100, 0x33);
        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ClientByteMover.MoveChannelToStream(state, () => { }, cts.Token);

        // Stream write should have failed
    }

    [Fact(Timeout = 5000)]
    public async Task ClientByteMover_should_handle_cancelled_write_in_drain_pipe_to_stream()
    {
        var slowStream = new SlowStream();
        var state = new ClientState(slowStream);
        var closeCount = 0;

        WriteToChannel(state, 100, 0x99);
        state.OutboundWriter.TryComplete();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await ClientByteMover.MoveChannelToStream(state, () => Interlocked.Increment(ref closeCount), cts.Token);

        Assert.Equal(1, closeCount);
    }

    private sealed class ThrowingReadStream : MemoryStream
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            return new ValueTask<int>(Task.FromException<int>(new IOException("Simulated read failure")));
        }
    }
}