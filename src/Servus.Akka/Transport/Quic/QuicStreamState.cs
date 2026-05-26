namespace Servus.Akka.Transport.Quic;

internal enum StreamPhase
{
    Opening,
    Active,
    HalfClosedWrite,
    HalfClosedRead,
    Closed
}

internal sealed class QuicStreamState
{
    private StreamHandle? _handle;
    private Queue<TransportBuffer>? _openingBuffer = new();

    public QuicStreamState(StreamDirection direction)
    {
        Direction = direction;
        Phase = StreamPhase.Opening;
    }

    public StreamPhase Phase { get; private set; }
    public StreamDirection Direction { get; }
    public bool HasHandle => _handle is not null;
    public int PendingWriteCount => _openingBuffer?.Count ?? 0;
    public bool IsCompleteWritesDeferred { get; private set; }

    public void AttachHandle(StreamHandle handle)
    {
        _handle = handle;

        if (_openingBuffer is not null)
        {
            while (_openingBuffer.TryDequeue(out var buf))
            {
                _handle.Write(buf);
            }

            _openingBuffer = null;
        }

        if (IsCompleteWritesDeferred)
        {
            IsCompleteWritesDeferred = false;
            _handle.CompleteWrites();
            Phase = StreamPhase.HalfClosedWrite;
        }
        else
        {
            Phase = StreamPhase.Active;
        }
    }

    public void Write(TransportBuffer buffer)
    {
        if (_handle is null)
        {
            _openingBuffer?.Enqueue(buffer);
            return;
        }

        _handle.Write(buffer);
    }

    public void CompleteWrites()
    {
        switch (Phase)
        {
            case StreamPhase.Opening:
                IsCompleteWritesDeferred = true;
                return;
            case StreamPhase.Active:
                _handle?.CompleteWrites();
                Phase = StreamPhase.HalfClosedWrite;
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
        _handle?.Abort(errorCode);
        Phase = StreamPhase.Closed;
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
        if (_handle is not null)
        {
            await _handle.DisposeAsync().ConfigureAwait(false);
            _handle = null;
        }
    }
}