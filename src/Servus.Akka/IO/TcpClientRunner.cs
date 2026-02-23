using System.Buffers;
using System.Net.Sockets;
using System.Threading.Channels;
using Akka.Actor;

namespace Servus.Akka.IO;

public class TcpClientRunner : ReceiveActor
{
    private readonly TcpClient _client;
    private readonly IActorRef _handler;
    private readonly NetworkStream _stream;
    private readonly TcpClientState _state;
    private readonly CancellationTokenSource _cts = new();
    private readonly IActorRef _selfClosure;

    private record CloseConnection()
    {
        public static readonly CloseConnection Instance = new();
    }

    public TcpClientRunner(TcpClient client, int maxFrameSize, IActorRef handler,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? inboundChannel = null,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? outboundChannel = null)
    {
        _client = client;
        _handler = handler;

        _stream = _client.GetStream();
        _state = new TcpClientState(maxFrameSize, inboundChannel, outboundChannel);

        _selfClosure = Context.Self;

        Receive<CloseConnection>(_ =>
        {
            _cts.Cancel();
            _handler.Tell(new TcpDisconnected(_client.Client.RemoteEndPoint!));
            Context.Self.Tell(PoisonPill.Instance);
        });
    }

    protected override void PreStart()
    {
        base.PreStart();

        _handler.Tell(new TcpConnected(_client.Client.RemoteEndPoint!, _state.InboundReader, _state.OutboundWriter));

        _ = DoWriteToPipeAsync(_stream, _cts.Token);
        _ = ReadFromPipeAsync(_cts.Token);
        _ = DoWriteToSocketAsync(_cts.Token);
    }

    private async Task DoWriteToPipeAsync(Stream stream, CancellationToken ct)
    {
        Exception? pipeError = null;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var bytesRead = await stream.ReadAsync(_state.GetWriteMemory(), ct).ConfigureAwait(false);
                    if (bytesRead == 0)
                    {
                        _selfClosure.Tell(CloseConnection.Instance);
                        return;
                    }

                    _state.Pipe.Writer.Advance(bytesRead);
                }
                catch (OperationCanceledException)
                {
                    // no need to log here
                    return;
                }
                catch (Exception ex)
                {
                    pipeError = ex;
                    _selfClosure.Tell(CloseConnection.Instance);
                    return;
                }

                // make data available to PipeReader
                var result = await _state.Pipe.Writer.FlushAsync(ct);
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
            await _state.Pipe.Writer.CompleteAsync(pipeError).ConfigureAwait(false);
        }
    }

    private async Task ReadFromPipeAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await _state.Pipe.Reader.ReadAsync(ct);

                // PipeReader.ReadAsync can return with IsCanceled=true when the token is
                // cancelled rather than throwing OperationCanceledException. In that case
                // the buffer is empty and we must not write a zero-length entry into
                // _readsFromTransport. Advance past the empty buffer and exit cleanly.
                if (result.IsCanceled)
                {
                    _state.Pipe.Reader.AdvanceTo(result.Buffer.Start);
                    _selfClosure.Tell(CloseConnection.Instance);
                    return;
                }

                var buffer = result.Buffer;

                // consume this entire sequence by copying it into a pooled buffer
                var length = (int) buffer.Length;
                if (length > 0)
                {
                    var pooled = MemoryPool<byte>.Shared.Rent(length);
                    buffer.CopyTo(pooled.Memory.Span);
                    _state.InboundWriter.TryWrite((pooled, length));
                }

                // tell the pipe we're done with this data
                _state.Pipe.Reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    _selfClosure.Tell(CloseConnection.Instance);
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                _selfClosure.Tell(CloseConnection.Instance);
                return;
            }
            catch (Exception)
            {
                // PipeWriter was completed with an exception (e.g. socket IOException propagated
                // through DoWriteToPipeAsync). The faulted pipe surfaces as an exception here
                // rather than as result.IsCompleted, so we must handle it explicitly to ensure
                // ReadFinished is always self-told and BackgroundTasksCompleted can fire.
                _selfClosure.Tell(CloseConnection.Instance);
                return;
            }
        }
    }
    
    private async Task DoWriteToSocketAsync(CancellationToken ct)
    {
        while (!_state.OutboundReader.Completion.IsCompleted)
        {
            try
            {
                while (await _state.OutboundReader.WaitToReadAsync(ct).ConfigureAwait(false))
                while (_state.OutboundReader.TryRead(out var item))
                {
                    var (buffer, readableBytes) = item;
                    try
                    {
                        var workingBuffer = buffer.Memory;
                        while (readableBytes > 0 && _stream is not null)
                        {
                            var slice = workingBuffer.Slice(0, readableBytes);
                            await _stream!.WriteAsync(slice, ct).ConfigureAwait(false);
                            readableBytes = 0; // Stream.WriteAsync writes all bytes
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

        _state.OutboundWriter.TryComplete(); // can't write anymore either
    }
}
