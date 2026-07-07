using System.Diagnostics.CodeAnalysis;
using System.Net.Quic;
using Akka.Actor;

namespace Servus.Akka.Transport.Quic;

/// <summary>
/// Shared stream-lifecycle driver for both QUIC state machines (client + server). Owns the per-stream
/// dictionary, read pump (sync-budget + PipeTo), and all stream-level operations: open, write, complete,
/// reset, inbound accept, lease attach, and inbound completion. The host SM provides connection-level
/// context (handle, options) as method parameters and retains its own connection lifecycle.
/// </summary>
internal sealed class QuicStreamLifecycle
{
    private readonly IConnectionOperations _ops;
    private readonly IActorRef _self;
    private readonly IQuicStreamReadHost _host;
    private readonly int _maxSyncReads;
    private readonly Dictionary<StreamTarget, QuicStreamState> _streams = new();
    private int _syncReadBudget;

    public QuicStreamLifecycle(
        IConnectionOperations ops, IActorRef self, IQuicStreamReadHost host, int maxSyncReads)
    {
        _ops = ops;
        _self = self;
        _host = host;
        _maxSyncReads = maxSyncReads;
        _syncReadBudget = maxSyncReads;
    }

    internal int ActiveStreamCount => _streams.Count;

    public bool TryGetStream(StreamTarget id, [MaybeNullWhen(false)] out QuicStreamState state)
        => _streams.TryGetValue(id, out state);

    internal QuicStreamState? GetStream(long rawStreamId)
        => _streams.GetValueOrDefault(StreamTarget.FromId(rawStreamId));

    public void HandleOpenStream(
        StreamTarget streamId, StreamDirection direction,
        QuicConnectionHandle connectionHandle, TransportConnectionOptions? options)
    {
        var state = QuicStreamState.Rent(direction, options);
        _streams[streamId] = state;

        var sid = streamId.Value;
        connectionHandle.OpenStreamAsync(direction)
            .PipeTo(_self,
                success: result => new StreamLeaseAcquired(result.Stream, sid),
                failure: ex => new AcquisitionFailed(ex));

        _ops.OnSignalPullOutbound();
    }

    public void HandleMultiplexedData(MultiplexedData data)
    {
        if (_streams.TryGetValue(data.StreamId, out var state))
        {
            state.Write(data.Buffer);
        }
        else
        {
            data.Buffer.Dispose();
        }

        data.Return();
        _ops.OnSignalPullOutbound();
    }

    public void HandleCompleteWrites(StreamTarget streamId)
    {
        if (_streams.TryGetValue(streamId, out var state))
        {
            state.CompleteWrites();
            if (state.Phase == StreamPhase.Closed)
            {
                _streams.Remove(streamId);
                _ = state.DisposeAndReturnAsync();
            }
        }

        _ops.OnSignalPullOutbound();
    }

    public void HandleResetStream(StreamTarget streamId, long errorCode)
    {
        if (_streams.Remove(streamId, out var state))
        {
            state.Abort(errorCode);
            _ = state.DisposeAndReturnAsync();
            _ops.OnPushInbound(new StreamClosed(streamId, DisconnectReason.Error));
        }

        _ops.OnSignalPullOutbound();
    }

    public void OnStreamLeaseAcquired(Stream stream, long rawStreamId)
    {
        var streamId = StreamTarget.FromId(rawStreamId);
        if (!_streams.TryGetValue(streamId, out var state))
        {
            stream.Dispose();
            return;
        }

        state.AttachConnection(stream, rawStreamId);
        if (state.Direction == StreamDirection.Bidirectional)
        {
            RequestStreamRead(streamId);
        }

        _ops.OnPushInbound(new StreamOpened(streamId, state.Direction));
    }

    public void OnInboundStreamAccepted(Stream stream, long rawStreamId, TransportConnectionOptions? options)
    {
        var streamId = StreamTarget.FromId(rawStreamId);
        var direction = (rawStreamId & 0x02) != 0
            ? StreamDirection.Unidirectional
            : StreamDirection.Bidirectional;
        var state = QuicStreamState.Rent(direction, options);
        state.AttachConnection(stream, rawStreamId);
        _streams[streamId] = state;

        _ops.OnPushInbound(new ServerStreamAccepted(streamId, direction));
        RequestStreamRead(streamId);
    }

    public void RequestStreamRead(StreamTarget streamId)
    {
        if (!_streams.TryGetValue(streamId, out var state) || !state.IsAttached)
        {
            return;
        }

        if (state.ReadArmed)
        {
            return;
        }

        if (state.QuicStream is { ReadsClosed.IsCompleted: true })
        {
            OnInboundComplete(DisconnectReason.Graceful, streamId.Value);
            return;
        }

        state.ReadArmed = true;

        var readTask = state.ReceiveAsync();

        if (readTask.IsCompletedSuccessfully && _syncReadBudget > 0)
        {
            _syncReadBudget--;
            QuicStreamReads.OnReceiveCompleted(_host, state, readTask.Result, state.Epoch);
            return;
        }

        _syncReadBudget = _maxSyncReads;
        readTask.PipeTo(_self, success: state.ReadSuccess, failure: state.ReadFailure);
    }

    public void NotifyItemDelivered(StreamTarget streamId) => RequestStreamRead(streamId);

    public void OnInboundComplete(DisconnectReason reason, long rawStreamId)
    {
        var streamId = StreamTarget.FromId(rawStreamId);
        if (!_streams.TryGetValue(streamId, out var state))
        {
            return;
        }

        if (reason == DisconnectReason.Graceful)
        {
            state.OnReadCompleted();

            if (state.Phase == StreamPhase.Closed)
            {
                _streams.Remove(streamId);
                _ = state.DisposeAndReturnAsync();
            }

            _ops.OnPushInbound(new StreamReadCompleted(streamId));
        }
        else
        {
            _streams.Remove(streamId);
            _ = state.DisposeAndReturnAsync();
            _ops.OnPushInbound(new StreamClosed(streamId, reason));
        }
    }

    public void DisposeAllStreams()
    {
        foreach (var (_, state) in _streams)
        {
            _ = state.DisposeAndReturnAsync();
        }

        _streams.Clear();
    }

    public void DisposeAllStreamsWithNotification(DisconnectReason reason)
    {
        foreach (var (target, state) in _streams)
        {
            _ops.OnPushInbound(new StreamClosed(target, reason));
            _ = state.DisposeAndReturnAsync();
        }

        _streams.Clear();
    }

    internal QuicStreamState RegisterTestStream(
        long streamId, StreamDirection direction, TransportConnectionOptions? options)
    {
        var target = StreamTarget.FromId(streamId);
        var state = QuicStreamState.Rent(direction, options);
        state.ActivateWithoutConnection();
        state.ActivateDirectReadForTest(streamId);
        _streams[target] = state;
        return state;
    }
}
