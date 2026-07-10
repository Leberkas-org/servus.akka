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

    private StreamConnection? _connection;
    private Stream? _stream;
    private Queue<WireBuffer>? _openingBuffer;
    private Task? _drainTask;
    private long _streamId;
    private StreamDirection _direction;
    private TransportConnectionOptions? _options;

    public StreamPhase Phase { get; private set; }
    public StreamDirection Direction => _direction;
    public int PendingWriteCount => _openingBuffer?.Count ?? 0;
    public bool IsCompleteWritesDeferred { get; private set; }

    /// <summary>True once a connection has been attached; the read pump only re-arms while attached.</summary>
    internal bool IsAttached => _connection is not null;

    /// <summary>
    /// True while a read is in flight for this stream (armed by <c>RequestStreamRead</c>, cleared by the
    /// shared completion handler in <see cref="QuicStreamReads"/> before it decides whether to re-arm).
    /// Enforces at most ONE in-flight read per stream — the pull-gating discipline this type exists for.
    /// </summary>
    internal bool ReadArmed { get; set; }

    /// <summary>
    /// Monotonic rent generation. Bumped every time the object leaves the pool (<see cref="Rent"/>) — for
    /// both a fresh instance and a repooled one — so an event produced by a transform captured under an
    /// earlier rent can be detected as stale after the state has been re-rented for a NEW stream.
    /// </summary>
    public int Epoch { get; private set; }

    private Func<WireBuffer?, IQuicTransportEvent> _readSuccess = null!;
    private Func<Exception, IQuicTransportEvent> _readFailure = null!;

    /// <summary>
    /// Cached PipeTo transforms — pure wrappers that capture only <c>(this, Epoch)</c> and package the raw
    /// completion into an event. They run on IO-completion threads and deliberately touch NO mutable
    /// state; all buffer/lifecycle handling happens on the actor when the event is dispatched. This is
    /// the same model as <see cref="Tcp.ReadEventState"/> and keeps every field on this class
    /// actor-confined (no fences, no Interlocked). The transforms are per-epoch (refreshed in
    /// <see cref="Rent"/>), so every produced event carries the epoch it was armed under — still zero
    /// per-read allocation. Buffer ownership during an in-flight read lives inside
    /// <see cref="StreamConnection"/>, so there is no per-state pending-buffer bookkeeping here.
    /// </summary>
    internal Func<WireBuffer?, IQuicTransportEvent> ReadSuccess => _readSuccess;
    internal Func<Exception, IQuicTransportEvent> ReadFailure => _readFailure;

    private QuicStreamState()
    {
    }

    /// <summary>
    /// The underlying <see cref="System.Net.Quic.QuicStream"/> if the connection was attached via a
    /// QuicStream; null otherwise.
    /// </summary>
    internal QuicStream? QuicStream => _stream as QuicStream;

    internal long StreamId => _streamId;

    /// <summary>Receives once into a freshly rented, caller-owned buffer; null on EOF.</summary>
    public ValueTask<WireBuffer?> ReceiveAsync() => _connection!.ReceiveAsync();

    public static QuicStreamState Rent(StreamDirection direction, TransportConnectionOptions? options)
    {
        if (!Pool.TryRent(out var state))
        {
            state = new QuicStreamState();
        }

        state._direction = direction;
        state._options = options;
        state.Phase = StreamPhase.Opening;
        state.ReadArmed = false;

        // Bump the rent generation and refresh the per-epoch transforms. Incrementing here (rather than
        // only in ResetAfterDispose) covers both a fresh instance and a repooled one uniformly, so an
        // in-flight event captured under the previous rent is always detectable as stale.
        state.Epoch++;
        var epoch = state.Epoch;
        state._readSuccess = buffer => new StreamReceiveCompleted(state, buffer, epoch);
        state._readFailure = ex => new StreamReceiveFailed(state, ex, epoch);
        return state;
    }

    // Called after DisposeAsync() has already cleaned up unmanaged resources.
    // Clears reference fields so the pooled object doesn't retain stale state.
    private void ResetAfterDispose()
    {
        _openingBuffer = null;
        _connection = null;
        _stream = null;
        _drainTask = null;
        _streamId = 0;
        _direction = default;
        _options = null;
        IsCompleteWritesDeferred = false;
        Phase = StreamPhase.Opening;
        ReadArmed = false;
    }

    public async ValueTask DisposeAndReturnAsync()
    {
        // Quiesce the inbound side first: this awaits the settlement of any in-flight receive (its buffer
        // is disposed inside the receive path on cancellation), so tearing down and repooling here can
        // never race a read that is still writing into a pooled array. With that settled, ALWAYS repool —
        // the old never-repool teardown rule is gone.
        if (_connection is not null)
        {
            await _connection.QuiesceAsync().ConfigureAwait(false);
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
    /// Test-only: gives this state a stream id and marks it active without a real connection, so unit
    /// tests can drive the read-completion handling (transforms, shared handler) in isolation.
    /// </summary>
    internal void ActivateDirectReadForTest(long streamId)
    {
        _streamId = streamId;
        Phase = StreamPhase.Active;
    }

    public void AttachConnection(Stream stream, long rawStreamId = 0, Action<int>? onFlushed = null)
    {
        _stream = stream;
        _streamId = rawStreamId;

        // ONE path for both QuicStream and plain/test streams: a quic-aware StreamConnection. The initial
        // receive hint comes from the transport options (closing the old hardcoded 4 KB); the adaptive
        // hint lives inside the connection.
        _connection = new StreamConnection(stream, _options ?? new TransportConnectionOptions(), quicAware: true);

        // Wire the wire-flush callback BEFORE draining the deferred opening buffers below: the connection's
        // send loop starts immediately, so any buffer enqueued here can be flushed by that loop before this
        // method returns. Assigning OnFlushed first guarantees the first opening batch's flush ack is never
        // dropped by a still-null callback (R4 / H6 outbound-credit-leak regression).
        _connection.OnFlushed = onFlushed;

        if (_openingBuffer is not null)
        {
            while (_openingBuffer.TryDequeue(out var buf))
            {
                if (!_connection.TryEnqueue(buf))
                {
                    buf.Dispose();
                }
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

    public bool Write(WireBuffer buffer)
    {
        if (_connection is null)
        {
            (_openingBuffer ??= new Queue<WireBuffer>()).Enqueue(buffer);
            return false;
        }

        if (!_connection.TryEnqueue(buffer))
        {
            // Output side already completed/disposed — ownership stays with us, so release the buffer.
            buffer.Dispose();
        }

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
            // StreamConnection owns the underlying stream and disposes it in its own DisposeAsync, so the
            // stream is NOT disposed a second time here.
            await _connection.DisposeAsync().ConfigureAwait(false);
            _connection = null;
        }

        _stream = null;
    }
}
