using System.Buffers;
using System.IO.Pipelines;
using System.Net.Quic;
using System.Net.Sockets;

namespace Servus.Akka.Transport;

internal sealed class SocketPipeConnection : IAsyncDisposable
{
    private readonly Pipe _inputPipe;
    private readonly Pipe _outputPipe;
    private readonly Task _receiveLoop;
    private readonly Task _sendLoop;
    private readonly CancellationTokenSource _cts;
    private readonly Socket? _socket;

    public PipeReader InputReader => _inputPipe.Reader;
    public PipeWriter OutputWriter => _outputPipe.Writer;

    internal static SocketPipeConnection CreateInert()
    {
        var inputPipe = new Pipe();
        var outputPipe = new Pipe();
        var cts = new CancellationTokenSource();
        return new SocketPipeConnection(inputPipe, outputPipe, Task.CompletedTask, Task.CompletedTask, cts);
    }

    public async Task CompleteAndDrainOutputAsync()
    {
        await _outputPipe.Writer.CompleteAsync();
        await _sendLoop;
    }

    private SocketPipeConnection(
        Pipe inputPipe,
        Pipe outputPipe,
        Task receiveLoop,
        Task sendLoop,
        CancellationTokenSource cts,
        Socket? socket = null)
    {
        _inputPipe = inputPipe;
        _outputPipe = outputPipe;
        _receiveLoop = receiveLoop;
        _sendLoop = sendLoop;
        _cts = cts;
        _socket = socket;
    }

    public static SocketPipeConnection Create(Socket socket, SocketPipeConnectionOptions? options = null)
    {
        var opts = options ?? new SocketPipeConnectionOptions();

        // One batched IOQueue shard per connection drives both transport-side schedulers (the input
        // writer = receive loop, the output reader = send loop); the application side stays on the
        // ThreadPool. This caps scheduler fan-out and batches socket wakeups instead of paying one
        // ThreadPool item per pipe continuation.
        var ioQueue = IOQueue.GetNext();

        var inputPipe = new Pipe(new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: ioQueue,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.InputPauseWriterThreshold,
            resumeWriterThreshold: opts.InputResumeWriterThreshold,
            useSynchronizationContext: false));

        var outputPipe = new Pipe(new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            readerScheduler: ioQueue,
            writerScheduler: PipeScheduler.ThreadPool,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.OutputPauseWriterThreshold,
            resumeWriterThreshold: opts.OutputResumeWriterThreshold,
            useSynchronizationContext: false));

        var cts = new CancellationTokenSource();
        var ct = cts.Token;

        // Do NOT pass `ct` as the second argument to Task.Run: if DisposeAsync cancels the token
        // before the thread pool starts the delegate, Task.Run(f, ct) cancels the task without ever
        // running the body, so the loop's teardown catch + finally never run and DisposeAsync's
        // Task.WhenAll surfaces a TaskCanceledException. The loops already observe `ct` internally
        // and shut down cleanly; the stream overload below relies on the same contract.
        var receiveLoop = Task.Run(() => RunSocketReceiveLoop(socket, inputPipe.Writer, opts, ct));
        var sendLoop = Task.Run(() => RunSocketSendLoop(socket, outputPipe.Reader, ct));

        return new SocketPipeConnection(inputPipe, outputPipe, receiveLoop, sendLoop, cts, socket);
    }

    public static SocketPipeConnection Create(Stream stream, SocketPipeConnectionOptions? options = null)
    {
        var opts = options ?? new SocketPipeConnectionOptions();

        // Same batched-scheduler model as the socket path (see Create(Socket)): the stream read/write
        // loops are the transport side and run on a per-connection IOQueue shard.
        var ioQueue = IOQueue.GetNext();

        var inputPipe = new Pipe(new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: ioQueue,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.InputPauseWriterThreshold,
            resumeWriterThreshold: opts.InputResumeWriterThreshold,
            useSynchronizationContext: false));

        var outputPipe = new Pipe(new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            readerScheduler: ioQueue,
            writerScheduler: PipeScheduler.ThreadPool,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.OutputPauseWriterThreshold,
            resumeWriterThreshold: opts.OutputResumeWriterThreshold,
            useSynchronizationContext: false));

        var cts = new CancellationTokenSource();
        var ct = cts.Token;

        Task receiveLoop;
        Task sendLoop;

        if (stream is QuicStream quicStream)
        {
            receiveLoop = Task.Run(() => RunQuicReceiveLoop(quicStream, inputPipe.Writer, opts, ct));
            sendLoop = Task.Run(() => RunQuicSendLoop(quicStream, outputPipe.Reader, ct));
        }
        else
        {
            receiveLoop = Task.Run(() => RunStreamReceiveLoop(stream, inputPipe.Writer, opts, ct));
            sendLoop = Task.Run(() => RunStreamSendLoop(stream, outputPipe.Reader, ct));
        }

        return new SocketPipeConnection(inputPipe, outputPipe, receiveLoop, sendLoop, cts);
    }

    private static bool IsTeardownException(Exception ex) =>
        ex is OperationCanceledException or SocketException or IOException or ObjectDisposedException;

    private const int AdaptiveMinHint = 4 * 1024;
    private const int AdaptiveMaxHint = 128 * 1024;
    private const int ShrinkStreakThreshold = 2;

    private static void AdaptHint(int bytesRead, ref int currentHint, ref int shrinkStreak)
    {
        if (bytesRead >= currentHint * 3 / 4)
        {
            shrinkStreak = 0;
            if (currentHint < AdaptiveMaxHint)
            {
                currentHint = Math.Min(currentHint * 2, AdaptiveMaxHint);
            }
        }
        else if (bytesRead < currentHint / 4)
        {
            if (++shrinkStreak >= ShrinkStreakThreshold && currentHint > AdaptiveMinHint)
            {
                currentHint = Math.Max(currentHint / 2, AdaptiveMinHint);
                shrinkStreak = 0;
            }
        }
        else
        {
            shrinkStreak = 0;
        }
    }

    private static async Task RunSocketReceiveLoop(
        Socket socket,
        PipeWriter writer,
        SocketPipeConnectionOptions options,
        CancellationToken ct)
    {
        var receiver = new SocketAwaitable();
        var currentHint = options.ReceiveBufferHint;
        var shrinkStreak = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (options.WaitForData)
                {
                    await receiver.WaitForDataAsync(socket);
                }

                var buffer = writer.GetMemory(currentHint);
                var bytesRead = await receiver.ReceiveAsync(socket, buffer);

                if (bytesRead == 0)
                {
                    break;
                }

                AdaptHint(bytesRead, ref currentHint, ref shrinkStreak);

                writer.Advance(bytesRead);
                var flushTask = writer.FlushAsync(ct);
                var flush = flushTask.IsCompletedSuccessfully ? flushTask.Result : await flushTask;

                if (flush.IsCompleted || flush.IsCanceled)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (IsTeardownException(ex))
        {
            // noop
        }
        finally
        {
            await writer.CompleteAsync();
        }
    }

    private static async Task RunStreamReceiveLoop(
        Stream stream,
        PipeWriter writer,
        SocketPipeConnectionOptions options,
        CancellationToken ct)
    {
        var currentHint = options.ReceiveBufferHint;
        var shrinkStreak = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var buffer = writer.GetMemory(currentHint);
                var bytesRead = await stream.ReadAsync(buffer, ct);

                if (bytesRead == 0)
                {
                    break;
                }

                AdaptHint(bytesRead, ref currentHint, ref shrinkStreak);

                writer.Advance(bytesRead);
                var flushTask = writer.FlushAsync(ct);
                var flush = flushTask.IsCompletedSuccessfully ? flushTask.Result : await flushTask;

                if (flush.IsCompleted || flush.IsCanceled)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (IsTeardownException(ex))
        {
            // noop
        }
        finally
        {
            await writer.CompleteAsync();
        }
    }

    private static async Task RunSocketSendLoop(
        Socket socket,
        PipeReader reader,
        CancellationToken ct)
    {
        var sender = new SocketAwaitable();

        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Guarded sync fast-path: the output pipe usually already has buffered bytes, so the
                // read completes synchronously; read .Result without the awaiter dance. Never blocks.
                var readTask = reader.ReadAsync(ct);
                var result = readTask.IsCompletedSuccessfully ? readTask.Result : await readTask;
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    break;
                }

                // Multi-segment buffers (accumulated/pipelined writes) leave in a single vectored
                // socket send instead of one syscall per segment.
                await sender.SendAsync(socket, buffer);

                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (IsTeardownException(ex))
        {
            // noop
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }

    private static async Task RunStreamSendLoop(
        Stream stream,
        PipeReader reader,
        CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Guarded sync fast-path: the output pipe usually already has buffered bytes, so the
                // read completes synchronously; read .Result without the awaiter dance. Never blocks.
                var readTask = reader.ReadAsync(ct);
                var result = readTask.IsCompletedSuccessfully ? readTask.Result : await readTask;
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    break;
                }

                if (buffer.IsSingleSegment)
                {
                    await stream.WriteAsync(buffer.First, ct);
                }
                else
                {
                    // Coalesce all segments into one ArrayPool buffer so that TLS (SslStream)
                    // produces a single record + one kernel send instead of N syscalls per segment.
                    var length = (int)buffer.Length;
                    var rented = ArrayPool<byte>.Shared.Rent(length);
                    buffer.CopyTo(rented);
                    await stream.WriteAsync(rented.AsMemory(0, length), ct);
                    ArrayPool<byte>.Shared.Return(rented);
                }

                await stream.FlushAsync(ct);
                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (IsTeardownException(ex))
        {
            // noop
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }

    private static async Task RunQuicReceiveLoop(
        QuicStream stream,
        PipeWriter writer,
        SocketPipeConnectionOptions options,
        CancellationToken ct)
    {
        var currentHint = options.ReceiveBufferHint;
        var shrinkStreak = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (stream.ReadsClosed.IsCompleted)
                {
                    break;
                }

                var buffer = writer.GetMemory(currentHint);
                var bytesRead = await stream.ReadAsync(buffer, ct);

                if (bytesRead == 0)
                {
                    break;
                }

                AdaptHint(bytesRead, ref currentHint, ref shrinkStreak);

                writer.Advance(bytesRead);
                var flushTask = writer.FlushAsync(ct);
                var flush = flushTask.IsCompletedSuccessfully ? flushTask.Result : await flushTask;

                if (flush.IsCompleted || flush.IsCanceled)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (IsTeardownException(ex))
        {
            // noop
        }
        finally
        {
            await writer.CompleteAsync();
        }
    }

    private static async Task RunQuicSendLoop(
        QuicStream stream,
        PipeReader reader,
        CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Avoid crossing into QuicStream.WriteAsync when the peer has sent STOP_SENDING
                // (WritesClosed is complete). A write attempt after that always throws
                // QuicException(StreamAborted) — checking first eliminates the allocation.
                if (stream.WritesClosed.IsCompleted)
                {
                    break;
                }

                var readTask = reader.ReadAsync(ct);
                var result = readTask.IsCompletedSuccessfully ? readTask.Result : await readTask;
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    break;
                }

                if (buffer.IsSingleSegment)
                {
                    await stream.WriteAsync(buffer.First, ct);
                }
                else
                {
                    var length = (int)buffer.Length;
                    var rented = ArrayPool<byte>.Shared.Rent(length);
                    buffer.CopyTo(rented);
                    await stream.WriteAsync(rented.AsMemory(0, length), ct);
                    ArrayPool<byte>.Shared.Return(rented);
                }

                await stream.FlushAsync(ct);
                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (IsTeardownException(ex))
        {
            // noop
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        if (_socket != null)
        {
            try
            {
                _socket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception ex) when (IsTeardownException(ex))
            {
                // noop
            }

            _socket.Close();
        }

        _inputPipe.Writer.CancelPendingFlush();
        _outputPipe.Reader.CancelPendingRead();
        await _outputPipe.Writer.CompleteAsync();

        await Task.WhenAll(_receiveLoop, _sendLoop);

        _cts.Dispose();
    }
}
