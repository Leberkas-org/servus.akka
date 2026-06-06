using System.IO.Pipelines;
using System.Net.Quic;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Transport.Quic;

internal enum StreamPhase
{
    Opening,
    Active,
    HalfClosedWrite,
    HalfClosedRead,
    Closed
}

internal sealed class QuicStreamState(StreamDirection direction) : IAsyncDisposable
{
    private SocketPipeConnection? _connection;
    private Stream? _stream;
    private Queue<TransportBuffer>? _openingBuffer = new();
    private SequencePosition? _pendingAdvance;

    public StreamPhase Phase { get; private set; } = StreamPhase.Opening;
    public StreamDirection Direction { get; } = direction;
    public int PendingWriteCount => _openingBuffer?.Count ?? 0;
    public bool IsCompleteWritesDeferred { get; private set; }
    public PipeReader? InputReader => _connection?.InputReader;

    public void AttachConnection(Stream stream)
    {
        _stream = stream;
        _connection = SocketPipeConnection.Create(stream);

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
            _openingBuffer?.Enqueue(buffer);
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

    public void SetPendingAdvance(SequencePosition position)
    {
        _pendingAdvance = position;
    }

    public void AdvancePendingRead()
    {
        if (_pendingAdvance is { } pos && _connection is not null)
        {
            _pendingAdvance = null;
            _connection.InputReader.AdvanceTo(pos);
        }
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

        _ = _connection.CompleteAndDrainOutputAsync().ContinueWith(_ =>
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
        _pendingAdvance = null;
        DisposePendingWrites();
        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
            _connection = null;
        }

        _stream = null;
    }
}
