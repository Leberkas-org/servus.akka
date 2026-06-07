using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using Servus.Akka.Transport.Tcp;

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

        var inputPipe = new Pipe(new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.InputPauseWriterThreshold,
            resumeWriterThreshold: opts.InputResumeWriterThreshold,
            useSynchronizationContext: false));

        var outputPipe = new Pipe(new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.OutputPauseWriterThreshold,
            resumeWriterThreshold: opts.OutputResumeWriterThreshold,
            useSynchronizationContext: false));

        var cts = new CancellationTokenSource();
        var ct = cts.Token;

        var receiveLoop = Task.Run(() => RunSocketReceiveLoop(socket, inputPipe.Writer, opts, ct), ct);
        var sendLoop = Task.Run(() => RunSocketSendLoop(socket, outputPipe.Reader, ct), ct);

        return new SocketPipeConnection(inputPipe, outputPipe, receiveLoop, sendLoop, cts, socket);
    }

    public static SocketPipeConnection Create(Stream stream, SocketPipeConnectionOptions? options = null)
    {
        var opts = options ?? new SocketPipeConnectionOptions();

        var inputPipe = new Pipe(new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.InputPauseWriterThreshold,
            resumeWriterThreshold: opts.InputResumeWriterThreshold,
            useSynchronizationContext: false));

        var outputPipe = new Pipe(new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            minimumSegmentSize: opts.MinimumSegmentSize,
            pauseWriterThreshold: opts.OutputPauseWriterThreshold,
            resumeWriterThreshold: opts.OutputResumeWriterThreshold,
            useSynchronizationContext: false));

        var cts = new CancellationTokenSource();
        var ct = cts.Token;

        var receiveLoop = Task.Run(() => RunStreamReceiveLoop(stream, inputPipe.Writer, opts, ct));
        var sendLoop = Task.Run(() => RunStreamSendLoop(stream, outputPipe.Reader, ct));

        return new SocketPipeConnection(inputPipe, outputPipe, receiveLoop, sendLoop, cts);
    }

    private static bool IsTeardownException(Exception ex) =>
        ex is OperationCanceledException or SocketException or IOException or ObjectDisposedException;

    private static async Task RunSocketReceiveLoop(
        Socket socket,
        PipeWriter writer,
        SocketPipeConnectionOptions options,
        CancellationToken ct)
    {
        var receiver = new SocketAwaitable();

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (options.WaitForData)
                {
                    await receiver.WaitForDataAsync(socket);
                }

                var buffer = writer.GetMemory(options.ReceiveBufferHint);
                var bytesRead = await receiver.ReceiveAsync(socket, buffer);

                if (bytesRead == 0)
                {
                    break;
                }

                writer.Advance(bytesRead);
                var flush = await writer.FlushAsync(ct);

                if (flush.IsCompleted || flush.IsCanceled)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (IsTeardownException(ex))
        {
            _ = ex;
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
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var buffer = writer.GetMemory(options.ReceiveBufferHint);
                var bytesRead = await stream.ReadAsync(buffer, ct);

                if (bytesRead == 0)
                {
                    break;
                }

                writer.Advance(bytesRead);
                var flush = await writer.FlushAsync(ct);

                if (flush.IsCompleted || flush.IsCanceled)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (IsTeardownException(ex))
        {
            _ = ex;
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
                var result = await reader.ReadAsync(ct);
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    break;
                }

                if (buffer.IsSingleSegment)
                {
                    await sender.SendAsync(socket, buffer.First);
                }
                else
                {
                    foreach (var segment in buffer)
                    {
                        await sender.SendAsync(socket, segment);
                    }
                }

                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (IsTeardownException(ex))
        {
            _ = ex;
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
                var result = await reader.ReadAsync(ct);
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
                    foreach (var segment in buffer)
                    {
                        await stream.WriteAsync(segment, ct);
                    }
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
            _ = ex;
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
                _ = ex;
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
