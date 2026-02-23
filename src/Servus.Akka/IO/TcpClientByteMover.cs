using System.Buffers;
using System.IO.Pipelines;
using Akka.Actor;

namespace Servus.Akka.IO;


public sealed record CloseConnection()
{
    public static readonly CloseConnection Instance = new();
}

internal static class TcpClientByteMover
{
    internal static async Task MoveStreamToPipe(TcpClientState state, IActorRef runner, CancellationToken ct)
    {
        Exception? pipeError = null;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var bytesRead = await state.Stream.ReadAsync(state.GetWriteMemory(), ct).ConfigureAwait(false);
                    if (bytesRead == 0)
                    {
                        runner.Tell(CloseConnection.Instance);
                        return;
                    }

                    state.Pipe.Writer.Advance(bytesRead);
                }
                catch (OperationCanceledException)
                {
                    // no need to log here
                    return;
                }
                catch (Exception ex)
                {
                    pipeError = ex;
                    runner.Tell(CloseConnection.Instance);
                    return;
                }

                // make data available to PipeReader
                var result = await state.Pipe.Writer.FlushAsync(ct);
                if (result.IsCompleted)
                {
                    return;
                }
            }
        }
        finally
        {
            // Always complete the pipe writer on any exit path so that ReadFromPipeAsync
            // can detect writer completion via result.IsCompleted rather than depending
            // solely on CancellationToken callback timing. Without this, ReadFromPipeAsync
            // can stall indefinitely on a loaded CI system if the cancellation callback
            // dispatch is delayed by thread pool pressure.
            await state.Pipe.Writer.CompleteAsync(pipeError).ConfigureAwait(false);
        }
    }

    internal static async Task MovePipeToChannel(TcpClientState state, IActorRef runner, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await state.Pipe.Reader.ReadAsync(ct);
                if (result.IsCanceled)
                {
                    // PipeReader.ReadAsync can return with IsCanceled=true when the token is
                    // cancelled rather than throwing OperationCanceledException. In that case
                    // the buffer is empty and we must not write a zero-length entry into
                    // _readsFromTransport. Advance past the empty buffer and exit cleanly.
                    state.Pipe.Reader.AdvanceTo(result.Buffer.Start);
                    runner.Tell(CloseConnection.Instance);
                    return;
                }

                // consume this entire sequence by copying it into a pooled buffer
                var buffer = result.Buffer;
                var length = (int) buffer.Length;
                if (length > 0)
                {
                    var pooled = MemoryPool<byte>.Shared.Rent(length);
                    buffer.CopyTo(pooled.Memory.Span);
                    state.InboundWriter.TryWrite((pooled, length));
                }

                // tell the pipe we're done with this data
                state.Pipe.Reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    runner.Tell(CloseConnection.Instance);
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                runner.Tell(CloseConnection.Instance);
                return;
            }
            catch (Exception)
            {
                // PipeWriter was completed with an exception (e.g. socket IOException propagated
                // through DoWriteToPipeAsync). The faulted pipe surfaces as an exception here
                // rather than as result.IsCompleted, so we must handle it explicitly to ensure
                // ReadFinished is always self-told and BackgroundTasksCompleted can fire.
                runner.Tell(CloseConnection.Instance);
                return;
            }
        }
    }
    
    internal static async Task MoveChannelToStream(TcpClientState state, IActorRef runner, CancellationToken ct)
    {
        while (!state.OutboundReader.Completion.IsCompleted)
        {
            try
            {
                while (await state.OutboundReader.WaitToReadAsync(ct).ConfigureAwait(false))
                while (state.OutboundReader.TryRead(out var item))
                {
                    var (buffer, readableBytes) = item;
                    try
                    {
                        var workingBuffer = buffer.Memory;
                        while (readableBytes > 0 && state.Stream is not null)
                        {
                            var slice = workingBuffer[..readableBytes];
                            await state.Stream!.WriteAsync(slice, ct).ConfigureAwait(false);
                            readableBytes = 0; 
                        }
                    }
                    finally
                    {
                        // free the pooled buffer
                        buffer.Dispose();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // we're being shut down
                return;
            }
            catch (Exception ex)
            {
                return;
            }
        }

        state.OutboundWriter.TryComplete(); // can't write anymore either
    }
}