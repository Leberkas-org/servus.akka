using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace Servus.Akka.Transport.Tcp.Client;

internal static class ClientByteMover
{
    public static Task MoveStreamToChannel(ClientState state, Action onClose, CancellationToken ct)
    {
        var fillTask = FillPipeFromStream(state.Stream, state.InboundPipe.Writer, ct);
        var drainTask = DrainPipeToChannel(state.InboundPipe.Reader, state.InboundWriter, onClose, ct);
        return Task.WhenAll(fillTask, drainTask);
    }

    public static Task MoveChannelToStream(ClientState state, Action onClose, CancellationToken ct)
    {
        var fillTask = FillPipeFromChannel(state.OutboundReader, state.OutboundPipe.Writer, ct);
        var drainTask = DrainPipeToStream(state.OutboundPipe.Reader, state.Stream, state.OnWritesComplete, onClose, ct);
        return Task.WhenAll(fillTask, drainTask);
    }

    private static async Task FillPipeFromStream(Stream stream, PipeWriter writer, CancellationToken ct)
    {
        Exception? error = null;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var mem = writer.GetMemory(512 * 1024);
                int bytesRead;
                try
                {
                    bytesRead = await stream.ReadAsync(mem, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception)
                {
                    error = new AbruptCloseException();
                    return;
                }

                if (bytesRead == 0)
                {
                    return;
                }

                writer.Advance(bytesRead);
                var flush = await writer.FlushAsync(ct).ConfigureAwait(false);
                if (flush.IsCompleted || flush.IsCanceled)
                {
                    break;
                }
            }
        }
        finally
        {
            try
            {
                writer.Complete(error);
            }
            catch (InvalidOperationException)
            {
                // noop
            }
        }
    }

    private static async Task DrainPipeToChannel(PipeReader reader, ChannelWriter<TransportBuffer> channel,
        Action onClose, CancellationToken ct)
    {
        var abrupt = false;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(ct).ConfigureAwait(false);
                var buffer = result.Buffer;

                foreach (var segment in buffer)
                {
                    var tb = TransportBuffer.Rent(segment.Length);
                    segment.Span.CopyTo(tb.FullMemory.Span);
                    tb.Length = segment.Length;
                    if (!channel.TryWrite(tb))
                    {
                        tb.Dispose();
                    }
                }

                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    if (reader.TryRead(out var final) && !final.Buffer.IsEmpty)
                    {
                        reader.AdvanceTo(final.Buffer.End);
                    }

                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            onClose();
            return;
        }
        catch (AbruptCloseException)
        {
            abrupt = true;
            onClose();
            return;
        }
        catch (Exception)
        {
            abrupt = true;
            onClose();
            return;
        }
        finally
        {
            try
            {
                reader.Complete();
            }
            catch (InvalidOperationException)
            {
                // noop
            }

            if (abrupt)
            {
                channel.TryComplete(new AbruptCloseException());
            }
            else
            {
                channel.TryComplete();
            }
        }

        onClose();
    }

    private static async Task FillPipeFromChannel(ChannelReader<TransportBuffer> channel, PipeWriter writer,
        CancellationToken ct)
    {
        try
        {
            while (await channel.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                while (channel.TryRead(out var buf))
                {
                    try
                    {
                        var span = writer.GetSpan(buf.Length);
                        buf.Span.CopyTo(span);
                        writer.Advance(buf.Length);
                    }
                    finally
                    {
                        buf.Dispose();
                    }
                }

                await writer.FlushAsync(ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // noop
        }
        catch (Exception)
        {
            // noop
        }
        finally
        {
            try
            {
                writer.Complete();
            }
            catch (InvalidOperationException)
            {
                // noop
            }
        }
    }

    private static async Task DrainPipeToStream(PipeReader reader, Stream stream, Action? onWritesComplete,
        Action onClose, CancellationToken ct)
    {
        try
        {
            while (true)
            {
                ReadResult result;
                try
                {
                    result = await reader.ReadAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    onClose();
                    return;
                }
                catch (Exception)
                {
                    onClose();
                    return;
                }

                var buffer = result.Buffer;
                try
                {
                    if (!buffer.IsEmpty)
                    {
                        if (buffer.IsSingleSegment)
                        {
                            await stream.WriteAsync(buffer.First, ct).ConfigureAwait(false);
                        }
                        else
                        {
                            using var owner = MemoryPool<byte>.Shared.Rent((int)buffer.Length);
                            buffer.CopyTo(owner.Memory.Span);
                            await stream.WriteAsync(owner.Memory[..(int)buffer.Length], ct).ConfigureAwait(false);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    reader.AdvanceTo(buffer.End);
                    onClose();
                    return;
                }
                catch (Exception)
                {
                    reader.AdvanceTo(buffer.End);
                    onClose();
                    return;
                }

                reader.AdvanceTo(buffer.End);
                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        finally
        {
            try
            {
                reader.Complete();
            }
            catch (InvalidOperationException)
            {
                // noop
            }
        }

        onWritesComplete?.Invoke();
    }
}