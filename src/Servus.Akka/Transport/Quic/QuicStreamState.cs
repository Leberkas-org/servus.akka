using System.Buffers;
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

internal sealed class QuicStreamState : IAsyncDisposable
{
    private static readonly ObjectPool<QuicStreamState> Pool = new(256);

    private SocketPipeConnection? _connection;
    private Stream? _stream;
    private Queue<TransportBuffer>? _openingBuffer;
    private Task? _drainTask;
    private PipeReader? _cachedReader;
    private long _streamId;
    private StreamDirection _direction;
    private SocketPipeConnectionOptions? _pipeOptions;

    public StreamPhase Phase { get; private set; }
    public StreamDirection Direction => _direction;
    public int PendingWriteCount => _openingBuffer?.Count ?? 0;
    public bool IsCompleteWritesDeferred { get; private set; }
    public PipeReader? InputReader => _connection?.InputReader;

    /// <summary>
    /// The connection generation that will be stamped into the next <see cref="PipeStreamReadComplete"/>
    /// message. Set by the state machine immediately before calling PipeTo, so the cached delegate
    /// picks it up at invocation time without capturing it as a per-read local.
    /// </summary>
    internal int ReadGen;

    /// <summary>
    /// Cached per-stream success transform for PipeTo. Allocated once in
    /// <see cref="AttachConnection"/> (and in <see cref="ActivateWithoutConnection"/> for tests),
    /// reused for every subsequent read on this stream to avoid per-read closure allocations.
    /// </summary>
    internal Func<ReadResult, IQuicTransportEvent>? ReadSuccessTransform;

    /// <summary>
    /// Direct-read transform used when the stream is a <see cref="System.Net.Quic.QuicStream"/>.
    /// Converts a raw byte count into a <see cref="PipeStreamReadComplete"/> event, consuming and
    /// clearing <see cref="PendingReadBuffer"/> in the process.
    /// </summary>
    internal Func<int, IQuicTransportEvent>? DirectReadTransform;

    /// <summary>
    /// Cached per-stream failure transform for PipeTo on direct (QuicStream) reads. Allocated
    /// once in <see cref="AttachConnection"/>, reused for every subsequent read on this stream
    /// to avoid per-read closure allocations.
    /// </summary>
    internal Func<Exception, IQuicTransportEvent>? FailureReadTransform;

    /// <summary>
    /// The underlying <see cref="System.Net.Quic.QuicStream"/> if the connection was attached via
    /// a QuicStream; null otherwise.
    /// </summary>
    internal QuicStream? QuicStream => _stream as QuicStream;

    private TransportBuffer? _pendingReadBuffer;
    private int _readHint = 4 * 1024;
    private int _shrinkCount;

    public int ReadHint => _readHint;

    public void AdaptReadHint(int bytesRead)
    {
        if (bytesRead >= _readHint * 3 / 4)
        {
            _shrinkCount = 0;
            if (_readHint < 128 * 1024)
            {
                _readHint = Math.Min(_readHint * 2, 128 * 1024);
            }
        }
        else if (bytesRead < _readHint / 4)
        {
            _shrinkCount++;
            if (_shrinkCount >= 2 && _readHint > 4 * 1024)
            {
                _readHint = Math.Max(_readHint / 2, 4 * 1024);
                _shrinkCount = 0;
            }
        }
        else
        {
            _shrinkCount = 0;
        }
    }

    public void ResetReadHint()
    {
        _readHint = 4 * 1024;
        _shrinkCount = 0;
    }

    public static QuicStreamState Rent(StreamDirection direction, SocketPipeConnectionOptions? pipeOptions)
    {
        if (!Pool.TryRent(out var state))
        {
            state = new QuicStreamState();
        }

        state._direction = direction;
        state._pipeOptions = pipeOptions;
        state.Phase = StreamPhase.Opening;
        return state;
    }

    // Called after DisposeAsync() has already cleaned up unmanaged resources.
    // Clears reference fields so the pooled object doesn't retain stale closures.
    private void ResetAfterDispose()
    {
        _pendingReadBuffer = null;
        _openingBuffer = null;
        _connection = null;
        _stream = null;
        _drainTask = null;
        _cachedReader = null;
        _streamId = 0;
        _direction = default;
        _pipeOptions = null;
        DirectReadTransform = null!;
        ReadSuccessTransform = null!;
        FailureReadTransform = null;
        IsCompleteWritesDeferred = false;
        Phase = StreamPhase.Opening;
        ResetReadHint();
    }

    public async ValueTask DisposeAndReturnAsync()
    {
        await DisposeAsync().ConfigureAwait(false);
        ResetAfterDispose();
        Pool.Return(this);
    }

    internal TransportBuffer? PendingReadBuffer
    {
        get => _pendingReadBuffer;
        set => _pendingReadBuffer = value;
    }

    internal void DisposePendingReadBuffer()
    {
        var buf = _pendingReadBuffer;
        _pendingReadBuffer = null;
        buf?.Dispose();
    }

    private Func<ReadResult, IQuicTransportEvent> BuildReadSuccessTransform(long rawStreamId)
    {
        return result =>
        {
            TransportBuffer? buf = null;
            if (result.Buffer.Length > 0)
            {
                var length = (int)result.Buffer.Length;
                buf = TransportBuffer.Rent(length);
                result.Buffer.CopyTo(buf.FullMemory.Span);
                buf.Length = length;
            }

            _cachedReader!.AdvanceTo(result.Buffer.End);
            return new PipeStreamReadComplete(buf, rawStreamId, ReadGen, result.IsCompleted || result.IsCanceled);
        };
    }

    private Func<int, IQuicTransportEvent> BuildDirectReadTransform(long rawStreamId)
    {
        return bytesRead =>
        {
            var buf = _pendingReadBuffer;
            _pendingReadBuffer = null;
            if (bytesRead == 0 || buf is null)
            {
                buf?.Dispose();
                return new PipeStreamReadComplete(null, rawStreamId, ReadGen, true);
            }
            buf.Length = bytesRead;
            AdaptReadHint(bytesRead);
            return new PipeStreamReadComplete(buf, rawStreamId, ReadGen, false);
        };
    }

    private Func<Exception, IQuicTransportEvent> BuildFailureReadTransform()
    {
        return ex =>
        {
            DisposePendingReadBuffer();
            // QuicException on read means the peer closed or reset the stream (FIN/STOP_SENDING/RST_STREAM).
            // Treat it as a graceful completion to avoid propagating a 184B exception object per stream
            // through the error-handling path when the stream was already being torn down.
            if (ex is QuicException)
            {
                return new PipeStreamReadComplete(null!, _streamId, ReadGen, true);
            }
            return new PipeStreamReadFailed(ex, _streamId, ReadGen);
        };
    }

    internal void ActivateWithoutConnection()
    {
        Phase = StreamPhase.Active;
    }

    /// <summary>
    /// Test-only: initialises the direct-read transforms on this state without requiring a real
    /// <see cref="System.Net.Quic.QuicStream"/>. Allows unit tests to invoke
    /// <see cref="DirectReadTransform"/> and <see cref="FailureReadTransform"/> in isolation.
    /// </summary>
    internal void ActivateDirectReadForTest(long streamId)
    {
        _streamId = streamId;
        Phase = StreamPhase.Active;
        DirectReadTransform = BuildDirectReadTransform(streamId);
        FailureReadTransform = BuildFailureReadTransform();
    }

    public void AttachConnection(Stream stream, long rawStreamId = 0)
    {
        _stream = stream;
        _streamId = rawStreamId;

        if (stream is QuicStream qs)
        {
            _connection = SocketPipeConnection.CreateForQuic(qs, _pipeOptions);
            DirectReadTransform = BuildDirectReadTransform(rawStreamId);
            FailureReadTransform = BuildFailureReadTransform();
        }
        else
        {
            _connection = SocketPipeConnection.Create(stream, _pipeOptions);
            _cachedReader = _connection.InputReader;
            ReadSuccessTransform = BuildReadSuccessTransform(rawStreamId);
        }

        if (_openingBuffer is not null)
        {
            while (_openingBuffer.TryDequeue(out var buf))
            {
                WriteToOutputPipe(buf);
            }

            _openingBuffer = null;
            _ = FlushWrites();
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

    public bool Write(TransportBuffer buffer)
    {
        if (_connection is null)
        {
            (_openingBuffer ??= new Queue<TransportBuffer>()).Enqueue(buffer);
            return false;
        }

        WriteToOutputPipe(buffer);
        return true;
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
    }

    public ValueTask<FlushResult> FlushWrites()
    {
        return _connection!.OutputWriter.FlushAsync();
    }

    private void CompleteWritesInternal()
    {
        if (_connection is null)
        {
            return;
        }

        _drainTask = _connection.CompleteAndDrainOutputAsync().ContinueWith(
            static (_, state) =>
            {
                if (state is QuicStream qs)
                {
                    qs.CompleteWrites();
                }
            },
            _stream,
            TaskContinuationOptions.ExecuteSynchronously);
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
        DisposePendingReadBuffer();
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
