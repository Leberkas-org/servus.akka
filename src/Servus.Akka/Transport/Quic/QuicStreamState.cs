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
    private Queue<WireBuffer>? _openingBuffer;
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
    /// Cached PipeTo transforms — pure wrappers that only capture <c>this</c> and package the raw
    /// completion into an event. They run on IO-completion threads and deliberately touch NO mutable
    /// state; all buffer/lifecycle handling happens on the actor when the event is dispatched. This
    /// is the same model as <see cref="Tcp.Client.TcpConnectionStateMachine"/>'s read pump and keeps
    /// every field on this class actor-confined (no fences, no Interlocked).
    /// </summary>
    internal readonly Func<int, IQuicTransportEvent> DirectReadTransform;
    internal readonly Func<ReadResult, IQuicTransportEvent> PipeReadTransform;
    internal readonly Func<Exception, IQuicTransportEvent> FailureReadTransform;

    private QuicStreamState()
    {
        DirectReadTransform = bytesRead => new DirectStreamReadComplete(this, bytesRead);
        PipeReadTransform = result => new PipeStreamReadResult(this, result);
        FailureReadTransform = ex => new PipeStreamReadFailed(this, ex);
    }

    /// <summary>
    /// The underlying <see cref="System.Net.Quic.QuicStream"/> if the connection was attached via
    /// a QuicStream; null otherwise.
    /// </summary>
    internal QuicStream? QuicStream => _stream as QuicStream;

    internal long StreamId => _streamId;

    private int _shrinkCount;

    public int ReadHint { get; private set; } = 4 * 1024;

    // Read-pump state. Actor-confined: written by BeginDirectRead/BeginPipeRead (read dispatch),
    // CompleteRead (completion arrival) and DisposeAndReturnAsync (teardown) — all of which run on
    // the owning connection actor. The buffer an in-flight QuicStream.ReadAsync writes into must
    // survive until its completion EVENT is processed; disposing it at teardown while the read was
    // still writing was the double-dispose that poisoned the shared WireBuffer pool and
    // corrupted unrelated connections (repro: GaudiHTTP LargeDownloadRegressionSpec under H3 load).
    internal WireBuffer? PendingReadBuffer { get; set; }
    internal bool ReadInFlight { get; private set; }
    internal SequencePosition? PendingAdvance { get; set; }
    private bool _tornDownWithReadInFlight;

    /// <summary>
    /// Publishes the buffer an in-flight <see cref="System.Net.Quic.QuicStream.ReadAsync(Memory{byte}, CancellationToken)"/>
    /// writes into and marks the read as in flight. Until <see cref="CompleteRead"/> runs, teardown
    /// must not dispose the buffer (the read may still be writing into its memory) and this instance
    /// must not be returned to the pool.
    /// </summary>
    internal void BeginDirectRead(WireBuffer buffer)
    {
        PendingReadBuffer = buffer;
        ReadInFlight = true;
    }

    /// <summary>Marks a pipe-reader read as in flight (no pending buffer; lifecycle gating only).</summary>
    internal void BeginPipeRead() => ReadInFlight = true;

    /// <summary>
    /// Actor-side arrival of a read completion (success or failure). Returns false when the stream
    /// was torn down while the read was in flight — the pending buffer has then been released here
    /// and the caller must drop the event; the instance itself is GC-reclaimed (never repooled).
    /// </summary>
    internal bool CompleteRead(out WireBuffer? pendingBuffer)
    {
        ReadInFlight = false;
        pendingBuffer = PendingReadBuffer;
        PendingReadBuffer = null;

        if (_tornDownWithReadInFlight)
        {
            pendingBuffer?.Dispose();
            pendingBuffer = null;
            return false;
        }

        return true;
    }

    public void AdaptReadHint(int bytesRead)
    {
        if (bytesRead >= ReadHint * 3 / 4)
        {
            _shrinkCount = 0;
            if (ReadHint < 128 * 1024)
            {
                ReadHint = Math.Min(ReadHint * 2, 128 * 1024);
            }
        }
        else if (bytesRead < ReadHint / 4)
        {
            _shrinkCount++;
            if (_shrinkCount >= 2 && ReadHint > 4 * 1024)
            {
                ReadHint = Math.Max(ReadHint / 2, 4 * 1024);
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
        ReadHint = 4 * 1024;
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
    // Clears reference fields so the pooled object doesn't retain stale state.
    private void ResetAfterDispose()
    {
        PendingReadBuffer = null;
        PendingAdvance = null;
        ReadInFlight = false;
        _tornDownWithReadInFlight = false;
        _openingBuffer = null;
        _connection = null;
        _stream = null;
        _drainTask = null;
        _cachedReader = null;
        _streamId = 0;
        _direction = default;
        _pipeOptions = null;
        IsCompleteWritesDeferred = false;
        Phase = StreamPhase.Opening;
        ResetReadHint();
    }

    public async ValueTask DisposeAndReturnAsync()
    {
        // Decided synchronously on the calling (actor) thread, BEFORE the first await hop — the
        // continuation below runs on an arbitrary thread and must not make lifecycle decisions.
        if (ReadInFlight)
        {
            // The in-flight read's completion event still references this instance. CompleteRead
            // releases the pending buffer when it arrives on the actor; the instance itself is
            // deliberately never repooled (a late completion must not observe a re-rented state) —
            // a bounded, rare pool miss on teardown-with-read-in-flight.
            _tornDownWithReadInFlight = true;
            await DisposeAsync().ConfigureAwait(false);
            return;
        }

        await DisposeAsync().ConfigureAwait(false);
        ResetAfterDispose();
        Pool.Return(this);
    }

    internal void ActivateWithoutConnection()
    {
        Phase = StreamPhase.Active;
    }

    /// <summary>
    /// Test-only: gives this state a stream id and marks it active without a real connection, so
    /// unit tests can drive the read-completion handling (<see cref="CompleteRead"/>, transforms)
    /// in isolation.
    /// </summary>
    internal void ActivateDirectReadForTest(long streamId)
    {
        _streamId = streamId;
        Phase = StreamPhase.Active;
    }

    public void AttachConnection(Stream stream, long rawStreamId = 0)
    {
        _stream = stream;
        _streamId = rawStreamId;

        if (stream is QuicStream qs)
        {
            _connection = SocketPipeConnection.CreateForQuic(qs, _pipeOptions);
        }
        else
        {
            _connection = SocketPipeConnection.CreateWithStreamReader(stream, _pipeOptions);
            _cachedReader = _connection.InputReader;
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

    public bool Write(WireBuffer buffer)
    {
        if (_connection is null)
        {
            (_openingBuffer ??= new Queue<WireBuffer>()).Enqueue(buffer);
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

    private void WriteToOutputPipe(WireBuffer data)
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
        // Runs synchronously up to the first await, i.e. still on the calling (actor) thread.
        if (!ReadInFlight)
        {
            // No read in flight: an orphaned pending buffer is exclusively ours to release.
            PendingReadBuffer?.Dispose();
            PendingReadBuffer = null;
        }
        // else: the in-flight QuicStream.ReadAsync may still be WRITING into the buffer's memory —
        // disposing (= returning the array to the shared pool) here would corrupt whoever rents it
        // next. CompleteRead releases it when the completion event arrives on the actor.

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
