using System.Diagnostics.CodeAnalysis;
using System.Net;
using Akka.Actor;

namespace Servus.Akka.Transport.Quic.Listener;

internal sealed class QuicServerStateMachine(
    IConnectionOperations ops,
    IActorRef self,
    QuicConnectionHandle connectionHandle,
    ConnectionInfo connectionInfo,
    TransportConnectionOptions? connectionOptions = null) : IQuicStreamReadHost
{
    private const string MigrationCheckTimerKey = "migration-check";
    private const int MaxSyncReads = 8;
    private static readonly TimeSpan MigrationCheckInterval = TimeSpan.FromSeconds(5);

    private bool _upstreamFinished;
    private EndPoint? _lastRemoteEndPoint;
    private CancellationTokenSource? _acceptCts;

    private readonly Dictionary<StreamTarget, QuicStreamState> _streams = new();
    private int _syncReadBudget = MaxSyncReads;

    internal int ActiveStreamCount => _streams.Count;

    IConnectionOperations IQuicStreamReadHost.Ops => ops;

    bool IQuicStreamReadHost.TryGetStream(StreamTarget id, [MaybeNullWhen(false)] out QuicStreamState state)
        => _streams.TryGetValue(id, out state);

    void IQuicStreamReadHost.RequestStreamRead(StreamTarget streamId) => RequestStreamRead(streamId);

    void IQuicStreamReadHost.OnInboundComplete(DisconnectReason reason, long rawStreamId)
        => OnInboundComplete(reason, rawStreamId);

    void IQuicStreamReadHost.OnReadFailure(QuicStreamState state, Exception error)
        => OnInboundComplete(DisconnectReason.Error, state.StreamId);

    public void Start()
    {
        _lastRemoteEndPoint = connectionHandle.RemoteEndPoint();
        ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);

        StartAcceptLoop();

        ops.OnPushInbound(new TransportConnected(connectionInfo));
    }

    internal void Dispatch(IQuicTransportEvent evt)
    {
        switch (evt)
        {
            case StreamReceiveCompleted e:
                QuicStreamReads.OnReceiveCompleted(this, e.State, e.Buffer, e.Epoch);
                break;
            case StreamReceiveFailed e:
                QuicStreamReads.OnReceiveFailed(this, e.State, e.Error, e.Epoch);
                break;
            case InboundStreamAccepted e:
                OnInboundStreamAccepted(e.Stream, e.StreamId);
                break;
            case StreamLeaseAcquired e:
                OnStreamLeaseAcquired(e.Stream, e.StreamId);
                break;
            case MigrationDetected e:
                ops.OnPushInbound(new ConnectionMigrationDetected(e.OldEndPoint, e.NewEndPoint));
                break;
        }
    }

    public void HandlePush(ITransportOutbound item)
    {
        switch (item)
        {
            case OpenStream open:
                HandleOpenStream(open.StreamId, open.Direction);
                break;
            case MultiplexedData data:
                HandleMultiplexedData(data);
                break;
            case CompleteWrites cw:
                HandleCompleteWrites(cw.StreamId);
                break;
            case ResetStream rs:
                HandleResetStream(rs.StreamId, rs.ErrorCode);
                break;
            case DisconnectTransport:
                Cleanup();
                ops.OnCompleteStage();
                break;
        }
    }

    public void HandleUpstreamFinish()
    {
        _upstreamFinished = true;
        StopAcceptLoop();
        ops.OnCompleteStage();
    }

    public void HandleDownstreamFinish()
    {
        Cleanup();
    }

    public void OnTimer(string? timerKey)
    {
        if (timerKey == MigrationCheckTimerKey)
        {
            CheckForConnectionMigration();
            ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);
        }
    }

    public void PostStop()
    {
        ops.OnCancelTimer(MigrationCheckTimerKey);
        Cleanup();
    }

    private void HandleOpenStream(StreamTarget streamId, StreamDirection direction)
    {
        var state = QuicStreamState.Rent(direction, connectionOptions);
        _streams[streamId] = state;

        var sid = streamId.Value;
        connectionHandle.OpenStreamAsync(direction)
            .PipeTo(self,
                success: result => new StreamLeaseAcquired(result.Stream, sid),
                failure: ex => new AcquisitionFailed(ex));

        ops.OnSignalPullOutbound();
    }

    private void HandleMultiplexedData(MultiplexedData data)
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
        ops.OnSignalPullOutbound();
    }

    private void HandleCompleteWrites(StreamTarget streamId)
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

        ops.OnSignalPullOutbound();
    }

    private void HandleResetStream(StreamTarget streamId, long errorCode)
    {
        if (_streams.Remove(streamId, out var state))
        {
            state.Abort(errorCode);
            _ = state.DisposeAndReturnAsync();
            ops.OnPushInbound(new StreamClosed(streamId, DisconnectReason.Error));
        }

        ops.OnSignalPullOutbound();
    }

    private void OnStreamLeaseAcquired(Stream stream, long rawStreamId)
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

        ops.OnPushInbound(new StreamOpened(streamId, state.Direction));
    }

    private void OnInboundStreamAccepted(Stream stream, long rawStreamId)
    {
        var streamId = StreamTarget.FromId(rawStreamId);
        var direction = (rawStreamId & 0x02) != 0
            ? StreamDirection.Unidirectional
            : StreamDirection.Bidirectional;
        var state = QuicStreamState.Rent(direction, connectionOptions);
        state.AttachConnection(stream, rawStreamId);
        _streams[streamId] = state;

        ops.OnPushInbound(new ServerStreamAccepted(streamId, direction));
        RequestStreamRead(streamId);
    }

    internal QuicStreamState RegisterTestStream(long streamId, StreamDirection direction)
    {
        var target = StreamTarget.FromId(streamId);
        var state = QuicStreamState.Rent(direction, connectionOptions);
        state.ActivateWithoutConnection();
        state.ActivateDirectReadForTest(streamId);
        _streams[target] = state;
        return state;
    }

    internal QuicStreamState? GetStreamForTest(long streamId)
        => _streams.GetValueOrDefault(StreamTarget.FromId(streamId));

    /// <summary>
    /// Re-arms the read for a stream whose queued item has just been delivered to downstream (called
    /// from the stage's onPull dequeue site). No-op if the stream's read is already armed or the stream
    /// is gone — mirrors the identical guard inside <see cref="RequestStreamRead"/>.
    /// </summary>
    internal void NotifyItemDelivered(StreamTarget streamId) => RequestStreamRead(streamId);

    private void RequestStreamRead(StreamTarget streamId)
    {
        if (!_streams.TryGetValue(streamId, out var state) || !state.IsAttached)
        {
            return;
        }

        // At most ONE in-flight read per stream: a no-op if a read is already armed for it.
        if (state.ReadArmed)
        {
            return;
        }

        // Peer already closed the read side of the QuicStream — nothing more will arrive.
        if (state.QuicStream is { ReadsClosed.IsCompleted: true })
        {
            OnInboundComplete(DisconnectReason.Graceful, streamId.Value);
            return;
        }

        state.ReadArmed = true;

        var readTask = state.ReceiveAsync();

        if (readTask.IsCompletedSuccessfully && _syncReadBudget > 0)
        {
            // Sync fast-path: this completes on the actor thread within the SAME rent, so the state's
            // current Epoch is exactly the epoch this read would carry — the guard inside
            // OnReceiveCompleted is a no-op here, it can never be stale.
            _syncReadBudget--;
            QuicStreamReads.OnReceiveCompleted(this, state, readTask.Result, state.Epoch);
            return;
        }

        _syncReadBudget = MaxSyncReads;
        readTask.PipeTo(self, success: state.ReadSuccess, failure: state.ReadFailure);
    }

    private void OnInboundComplete(DisconnectReason reason, long rawStreamId)
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

            ops.OnPushInbound(new StreamReadCompleted(streamId));
        }
        else
        {
            _streams.Remove(streamId);
            _ = state.DisposeAndReturnAsync();
            ops.OnPushInbound(new StreamClosed(streamId, reason));
        }
    }


    private void CheckForConnectionMigration()
    {
        var currentRemote = connectionHandle.RemoteEndPoint();
        if (currentRemote is null || _lastRemoteEndPoint is null)
        {
            return;
        }

        if (!currentRemote.Equals(_lastRemoteEndPoint))
        {
            var old = _lastRemoteEndPoint;
            _lastRemoteEndPoint = currentRemote;
            ops.OnPushInbound(new ConnectionMigrationDetected(old, currentRemote));
        }
    }

    private void StartAcceptLoop()
    {
        _acceptCts?.Cancel();
        _acceptCts?.Dispose();
        _acceptCts = new CancellationTokenSource();
        _ = AcceptLoopAsync(connectionHandle, self, _acceptCts.Token);
    }

    private void StopAcceptLoop()
    {
        _acceptCts?.Cancel();
        _acceptCts?.Dispose();
        _acceptCts = null;
    }

    private static async Task AcceptLoopAsync(
        QuicConnectionHandle handle, IActorRef self, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await handle.AcceptInboundStreamAsync(ct).ConfigureAwait(false);

                if (ct.IsCancellationRequested)
                {
                    if (result is not null)
                    {
                        await result.Value.Stream.DisposeAsync().ConfigureAwait(false);
                    }

                    return;
                }

                if (result is null)
                {
                    continue;
                }

                self.Tell(new InboundStreamAccepted(result.Value.Stream, result.Value.StreamId));
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (Exception)
            {
                return;
            }
        }
    }

    private void Cleanup()
    {
        StopAcceptLoop();

        foreach (var (_, state) in _streams)
        {
            _ = state.DisposeAndReturnAsync();
        }

        _streams.Clear();

        _ = connectionHandle.DisposeAsync();
    }
}