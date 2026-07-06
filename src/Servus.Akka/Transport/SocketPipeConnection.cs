using System.Buffers;
using System.IO.Pipelines;
using System.Net.Quic;
using System.Net.Sockets;

namespace Servus.Akka.Transport;

internal sealed class SocketPipeConnection : IAsyncDisposable
{
    private readonly Pipe? _inputPipe;
    private readonly Pipe _outputPipe;
    private readonly Task _receiveLoop;
    private readonly Task _sendLoop;
    private readonly CancellationTokenSource _cts;
    private readonly Socket? _socket;
    private readonly PipeReader? _directInputReader;
    private readonly Stream? _receiveStream;
    private readonly SocketAwaitable? _receiver;
    private readonly bool _waitForData;
    private int _receiveHint;
    private int _shrinkStreak;
    private int _receiveActive;

    public PipeReader InputReader =>
        _directInputReader
        ?? _inputPipe?.Reader
        ?? throw new InvalidOperationException("TCP connections receive via ReceiveAsync().");
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

    private SocketPipeConnection(
        PipeReader directInputReader,
        Pipe outputPipe,
        Task sendLoop,
        CancellationTokenSource cts)
    {
        _directInputReader = directInputReader;
        _outputPipe = outputPipe;
        _sendLoop = sendLoop;
        _cts = cts;
        // _inputPipe stays default (unused)
        _receiveLoop = Task.CompletedTask;
    }

    private SocketPipeConnection(
        Socket? socket,
        Stream? receiveStream,
        Pipe outputPipe,
        Task sendLoop,
        CancellationTokenSource cts,
        SocketPipeConnectionOptions opts)
    {
        _socket = socket;
        _receiveStream = receiveStream;
        _receiver = socket is not null ? new SocketAwaitable() : null;
        _outputPipe = outputPipe;
        _sendLoop = sendLoop;
        _cts = cts;
        _waitForData = opts.WaitForData;
        _receiveHint = opts.ReceiveBufferHint;
        _receiveLoop = Task.CompletedTask;
    }

    public static SocketPipeConnection CreateForQuic(QuicStream stream, SocketPipeConnectionOptions? options = null)
    {
        var opts = options ?? new SocketPipeConnectionOptions();
        var ioQueue = IOQueue.GetNext();

        var fallbackReader = PipeReader.Create(stream, new StreamPipeReaderOptions(
            pool: CrossThreadMemoryPool.Instance,
            bufferSize: opts.MinimumSegmentSize,
            leaveOpen: true));

        var outputPipe = new Pipe(new PipeOptions(
            pool: CrossThreadMemoryPool.Instance,
            readerScheduler: ioQueue,
            writerScheduler: PipeScheduler.ThreadPool,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.OutputPauseWriterThreshold,
            resumeWriterThreshold: opts.OutputResumeWriterThreshold,
            useSynchronizationContext: false));

        var cts = new CancellationTokenSource();

        // See Create(Socket) for why ct is NOT passed to Task.Run itself.
        var sendLoop = Task.Run(() => RunQuicSendLoop(stream, outputPipe.Reader, cts.Token));

        return new SocketPipeConnection(fallbackReader, outputPipe, sendLoop, cts);
    }

    // QUIC stream states can attach plain (non-QuicStream) streams — e.g. transformed or test
    // streams — and read them through the pipe-based InputReader path. TCP's direct
    // rent-and-receive mode does not apply there, so this factory keeps the StreamPipeReader
    // mechanism CreateForQuic uses, paired with the generic stream send loop.
    internal static SocketPipeConnection CreateWithStreamReader(Stream stream, SocketPipeConnectionOptions? options = null)
    {
        var opts = options ?? new SocketPipeConnectionOptions();
        var ioQueue = IOQueue.GetNext();

        var reader = PipeReader.Create(stream, new StreamPipeReaderOptions(
            pool: CrossThreadMemoryPool.Instance,
            bufferSize: opts.MinimumSegmentSize,
            leaveOpen: true));

        var outputPipe = new Pipe(new PipeOptions(
            pool: CrossThreadMemoryPool.Instance,
            readerScheduler: ioQueue,
            writerScheduler: PipeScheduler.ThreadPool,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.OutputPauseWriterThreshold,
            resumeWriterThreshold: opts.OutputResumeWriterThreshold,
            useSynchronizationContext: false));

        var cts = new CancellationTokenSource();

        // See Create(Socket) for why ct is NOT passed to Task.Run itself.
        var sendLoop = Task.Run(() => RunStreamSendLoop(stream, outputPipe.Reader, cts.Token));

        return new SocketPipeConnection(reader, outputPipe, sendLoop, cts);
    }

    public static SocketPipeConnection Create(Socket socket, SocketPipeConnectionOptions? options = null)
    {
        var opts = options ?? new SocketPipeConnectionOptions();

        // Inbound is direct rent-and-receive (ReceiveAsync) — no input pipe, no receive loop. The
        // kernel socket buffer is the read-ahead; an unread backlog closes the TCP window, which is
        // the inbound backpressure. Only the outbound side still runs through a pipe + send loop.
        var ioQueue = IOQueue.GetNext();

        var outputPipe = new Pipe(new PipeOptions(
            pool: CrossThreadMemoryPool.Instance,
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
        // await surfaces a TaskCanceledException. The loop already observes `ct` internally.
        var sendLoop = Task.Run(() => RunSocketSendLoop(socket, outputPipe.Reader, ct));

        return new SocketPipeConnection(socket, receiveStream: null, outputPipe, sendLoop, cts, opts);
    }

    public static SocketPipeConnection Create(Stream stream, SocketPipeConnectionOptions? options = null)
    {
        var opts = options ?? new SocketPipeConnectionOptions();
        var ioQueue = IOQueue.GetNext();

        var outputPipe = new Pipe(new PipeOptions(
            pool: CrossThreadMemoryPool.Instance,
            readerScheduler: ioQueue,
            writerScheduler: PipeScheduler.ThreadPool,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.OutputPauseWriterThreshold,
            resumeWriterThreshold: opts.OutputResumeWriterThreshold,
            useSynchronizationContext: false));

        var cts = new CancellationTokenSource();
        var ct = cts.Token;

        var sendLoop = Task.Run(() => RunStreamSendLoop(stream, outputPipe.Reader, ct));

        return new SocketPipeConnection(socket: null, stream, outputPipe, sendLoop, cts, opts);
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

    /// <summary>
    /// Receives once, directly into a rented <see cref="WireBuffer"/> owned by the caller.
    /// Returns null on EOF. Not reentrant: at most one outstanding call per connection — the
    /// state machines' pull discipline guarantees this. On failure the rent is disposed here
    /// and the exception propagates.
    /// </summary>
    public async ValueTask<WireBuffer?> ReceiveAsync()
    {
        if (Interlocked.Exchange(ref _receiveActive, 1) == 1)
        {
            throw new InvalidOperationException(
                "Concurrent ReceiveAsync — the connection supports one outstanding receive.");
        }

        try
        {
            if (_socket is null && _receiveStream is null)
            {
                // Inert connection (tests): park until disposed, mirroring a pipe read that never
                // completes. Cancellation surfaces as a teardown-classified exception.
                await Task.Delay(Timeout.Infinite, _cts.Token);
                return null;
            }

            if (_socket is not null && _waitForData)
            {
                // Zero-byte receive before renting so idle connections don't pin a rented buffer.
                await _receiver!.WaitForDataAsync(_socket);
            }

            var buffer = WireBuffer.Rent(_receiveHint);
            try
            {
                var bytesRead = _socket is not null
                    ? await _receiver!.ReceiveAsync(_socket, buffer.FullMemory)
                    : await _receiveStream!.ReadAsync(buffer.FullMemory, _cts.Token);

                if (bytesRead == 0)
                {
                    buffer.Dispose();
                    return null;
                }

                buffer.Length = bytesRead;
                AdaptHint(bytesRead, ref _receiveHint, ref _shrinkStreak);
                return buffer;
            }
            catch
            {
                buffer.Dispose();
                throw;
            }
        }
        finally
        {
            Volatile.Write(ref _receiveActive, 0);
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
                    // Coalesce all segments into one buffer so that TLS (SslStream) produces a single
                    // record + one kernel send instead of N syscalls per segment. Rent from the bounded
                    // cross-thread pool, NOT ArrayPool<byte>.Shared: the per-core Shared pool thrashes
                    // and allocates fresh arrays under the transport's concurrent send load.
                    var length = (int)buffer.Length;
                    using var owner = WireBuffer.Rent(length);
                    var mem = owner.FullMemory[..length];
                    buffer.CopyTo(mem.Span);
                    await stream.WriteAsync(mem, ct);
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
                    // Cross-thread pool, not ArrayPool<byte>.Shared: under concurrent QUIC stream sends
                    // the per-core Shared pool misses constantly and allocates fresh byte[] (the bulk of
                    // the H3 upload allocation). The bounded shared pool absorbs the churn.
                    var length = (int)buffer.Length;
                    using var owner = WireBuffer.Rent(length);
                    var mem = owner.FullMemory[..length];
                    buffer.CopyTo(mem.Span);
                    await stream.WriteAsync(mem, ct);
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

        if (_directInputReader is not null)
        {
            await _directInputReader.CompleteAsync();
        }
        else
        {
            _inputPipe?.Writer.CancelPendingFlush();
        }

        _outputPipe.Reader.CancelPendingRead();
        await _outputPipe.Writer.CompleteAsync();
        await Task.WhenAll(_receiveLoop, _sendLoop);

        _cts.Dispose();
    }
}
