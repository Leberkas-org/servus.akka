using System.IO.Pipelines;
using System.Net.Quic;
using static Servus.Senf;

namespace Servus.Akka.Transport.Quic;

internal enum StreamPhase
{
    Opening,
    Active,
    HalfClosedWrite,
    HalfClosedRead,
    Closed
}

internal sealed class QuicStreamState(StreamDirection direction, SocketPipeConnectionOptions? pipeOptions = null) : IAsyncDisposable
{
    private SocketPipeConnection? _connection;
    private Stream? _stream;
    private Queue<TransportBuffer>? _openingBuffer;
    private Task? _drainTask;

    public StreamPhase Phase { get; private set; } = StreamPhase.Opening;
    public StreamDirection Direction { get; } = direction;
    public int PendingWriteCount => _openingBuffer?.Count ?? 0;
    public bool IsCompleteWritesDeferred { get; private set; }
    public PipeReader? InputReader => _connection?.InputReader;

    internal void ActivateWithoutConnection()
    {
        Phase = StreamPhase.Active;
    }

    public void AttachConnection(Stream stream)
    {
        _stream = stream;
        _connection = SocketPipeConnection.Create(stream, pipeOptions);

        if (_openingBuffer is not null)
        {
            while (_openingBuffer.TryDequeue(out var buf))
            {
                WriteToOutputPipe(buf);
            }

            _openingBuffer = null;
        }

        if (IsCompleteWritesDeferred)
        {
            IsCompleteWritesDeferred = false;
            CompleteWritesInternal();
            Phase = StreamPhase.HalfClosedWrite;
        }
        else
        {
            Phase = StreamPhase.Active;
        }
    }

    public void Write(TransportBuffer buffer)
    {
        if (_connection is null)
        {
            (_openingBuffer ??= new Queue<TransportBuffer>()).Enqueue(buffer);
            return;
        }

        WriteToOutputPipe(buffer);
    }

    public void CompleteWrites()
    {
        switch (Phase)
        {
            case StreamPhase.Opening:
                IsCompleteWritesDeferred = true;
                return;
            case StreamPhase.Active:
                CompleteWritesInternal();
                Phase = StreamPhase.HalfClosedWrite;
                return;
            case StreamPhase.HalfClosedRead:
                CompleteWritesInternal();
                Phase = StreamPhase.Closed;
                return;
        }
    }

    public void OnReadCompleted()
    {
        Phase = Phase switch
        {
            StreamPhase.Active => StreamPhase.HalfClosedRead,
            StreamPhase.HalfClosedWrite => StreamPhase.Closed,
            _ => Phase
        };
    }

    public void Abort(long errorCode)
    {
        if (_stream is QuicStream qs)
        {
            qs.Abort(QuicAbortDirection.Both, errorCode);
        }

        Phase = StreamPhase.Closed;
    }

    public Task CompleteAndDrainOutputAsync()
    {
        return _connection?.CompleteAndDrainOutputAsync() ?? Task.CompletedTask;
    }

    private void WriteToOutputPipe(TransportBuffer data)
    {
        var writer = _connection!.OutputWriter;
        var mem = writer.GetMemory(data.Length);
        data.Memory.Span.CopyTo(mem.Span);
        writer.Advance(data.Length);
        data.Dispose();
        _ = writer.FlushAsync();
    }

    private void CompleteWritesInternal()
    {
        if (_connection is null)
        {
            return;
        }

        _drainTask = _connection.CompleteAndDrainOutputAsync().ContinueWith(_ =>
        {
            if (_stream is QuicStream qs)
            {
                qs.CompleteWrites();
            }
        }, TaskContinuationOptions.ExecuteSynchronously);
    }

    private void DisposePendingWrites()
    {
        if (_openingBuffer is null)
        {
            return;
        }

        while (_openingBuffer.TryDequeue(out var orphan))
        {
            orphan.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        DisposePendingWrites();

        if (_drainTask is not null)
        {
            try
            {
                await _drainTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Tracing.For("Connection").Debug(this, "drain failed during disposal: {0}", ex.Message);
            }

            _drainTask = null;
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
            _connection = null;
        }

        if (_stream is not null)
        {
            try
            {
                await _stream.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Tracing.For("Connection").Debug(this, "stream disposal failed: {0}", ex.Message);
            }

            _stream = null;
        }
    }
}
