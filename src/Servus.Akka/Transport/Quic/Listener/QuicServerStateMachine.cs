using System.Net;
using Akka.Actor;

namespace Servus.Akka.Transport.Quic.Listener;

internal sealed class QuicServerStateMachine
{
    private const string MigrationCheckTimerKey = "migration-check";
    private static readonly TimeSpan MigrationCheckInterval = TimeSpan.FromSeconds(5);

    private readonly ITransportOperations _ops;
    private readonly IActorRef _self;
    private readonly QuicConnectionHandle _connectionHandle;
    private readonly ConnectionInfo _connectionInfo;

    private int _connectionGen;
    private bool _upstreamFinished;
    private EndPoint? _lastRemoteEndPoint;

    private readonly Dictionary<StreamTarget, QuicStreamState> _streams = new();
    private QuicPumpManager? _pumpManager;

    public QuicServerStateMachine(
        ITransportOperations ops,
        IActorRef self,
        QuicConnectionHandle connectionHandle,
        ConnectionInfo connectionInfo)
    {
        _ops = ops;
        _self = self;
        _connectionHandle = connectionHandle;
        _connectionInfo = connectionInfo;
    }

    public void Start()
    {
        _connectionGen++;
        _lastRemoteEndPoint = _connectionHandle.RemoteEndPoint();
        _ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);

        _pumpManager = new QuicPumpManager(_self);
        _pumpManager.StartAcceptLoop(_connectionHandle);

        _ops.OnPushInbound(new TransportConnected(_connectionInfo));
    }

    internal void Dispatch(IQuicTransportEvent evt)
    {
        switch (evt)
        {
            case InboundData e:
                if (e.Gen == _connectionGen)
                {
                    _ops.OnPushInbound(new MultiplexedData(e.Buffer, StreamTarget.FromId(e.StreamId)));
                }
                else
                {
                    e.Buffer.Dispose();
                }
                break;
            case InboundStreamAccepted e:
                OnInboundStreamAccepted(e.Stream, e.StreamId);
                break;
            case StreamLeaseAcquired e:
                OnStreamLeaseAcquired(e.Handle, e.StreamId);
                break;
            case InboundComplete e:
                if (e.Gen == _connectionGen)
                {
                    OnInboundComplete(e.Reason, e.StreamId);
                }
                break;
            case InboundPumpFailed e:
                OnInboundComplete(DisconnectReason.Error, e.StreamId);
                break;
            case OutboundWriteDone:
                _ops.OnSignalPullOutbound();
                break;
            case OutboundWriteFailed:
                HandleConnectionFailure(DisconnectReason.Error);
                break;
            case MigrationDetected e:
                _ops.OnPushInbound(new ConnectionMigrationDetected(e.OldEndPoint, e.NewEndPoint));
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
                _ops.OnCompleteStage();
                break;
        }
    }

    public void HandleUpstreamFinish()
    {
        _upstreamFinished = true;
        _pumpManager?.StopAll();
        _ops.OnCompleteStage();
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
            _ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);
        }
    }

    public void PostStop()
    {
        _ops.OnCancelTimer(MigrationCheckTimerKey);
        Cleanup();
    }

    private void HandleOpenStream(StreamTarget streamId, StreamDirection direction)
    {
        var state = new QuicStreamState(direction);
        _streams[streamId] = state;

        var sid = streamId.Value;
        _connectionHandle.OpenStreamAsync(direction)
            .PipeTo(_self,
                success: result => new StreamLeaseAcquired(new StreamHandle(result.Stream), sid),
                failure: ex => new AcquisitionFailed(ex));

        _ops.OnSignalPullOutbound();
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

        _ops.OnSignalPullOutbound();
    }

    private void HandleCompleteWrites(StreamTarget streamId)
    {
        if (_streams.TryGetValue(streamId, out var state))
        {
            state.CompleteWrites();
        }

        _ops.OnSignalPullOutbound();
    }

    private void HandleResetStream(StreamTarget streamId, long errorCode)
    {
        if (_streams.Remove(streamId, out var state))
        {
            state.Abort(errorCode);
            _ = state.DisposeAsync();
            _ops.OnPushInbound(new StreamClosed(streamId, DisconnectReason.Error));
        }

        _ops.OnSignalPullOutbound();
    }

    private void OnStreamLeaseAcquired(StreamHandle handle, long rawStreamId)
    {
        var streamId = StreamTarget.FromId(rawStreamId);
        if (!_streams.TryGetValue(streamId, out var state))
        {
            _ = handle.DisposeAsync();
            return;
        }

        state.AttachHandle(handle);
        _pumpManager?.StartInboundPump(handle, rawStreamId, _connectionGen);
        _ops.OnPushInbound(new StreamOpened(streamId, state.Direction));
    }

    private void OnInboundStreamAccepted(Stream stream, long rawStreamId)
    {
        var streamId = StreamTarget.FromId(rawStreamId);
        var handle = new StreamHandle(stream);
        var state = new QuicStreamState(StreamDirection.Unidirectional);
        state.AttachHandle(handle);
        _streams[streamId] = state;

        _pumpManager?.StartInboundPump(handle, rawStreamId, _connectionGen);
        _ops.OnPushInbound(new ServerStreamAccepted(streamId, StreamDirection.Unidirectional));
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
                _ = state.DisposeAsync();
            }

            _ops.OnPushInbound(new StreamReadCompleted(streamId));
        }
        else
        {
            _streams.Remove(streamId);
            _ = state.DisposeAsync();
            _ops.OnPushInbound(new StreamClosed(streamId, reason));
        }
    }

    private void HandleConnectionFailure(DisconnectReason reason)
    {
        foreach (var (target, state) in _streams)
        {
            _ops.OnPushInbound(new StreamClosed(target, reason));
            _ = state.DisposeAsync();
        }

        _streams.Clear();

        _ops.OnPushInbound(new TransportDisconnected(reason));
        _pumpManager?.StopAll();

        if (_upstreamFinished)
        {
            _ops.OnCompleteStage();
        }
        else
        {
            _ops.OnSignalPullOutbound();
        }
    }

    private void CheckForConnectionMigration()
    {
        var currentRemote = _connectionHandle.RemoteEndPoint();
        if (currentRemote is null || _lastRemoteEndPoint is null)
        {
            return;
        }

        if (!currentRemote.Equals(_lastRemoteEndPoint))
        {
            var old = _lastRemoteEndPoint;
            _lastRemoteEndPoint = currentRemote;
            _ops.OnPushInbound(new ConnectionMigrationDetected(old, currentRemote));
        }
    }

    private void Cleanup()
    {
        _connectionGen++;
        _pumpManager?.StopAll();
        _pumpManager = null;

        foreach (var (_, state) in _streams)
        {
            _ = state.DisposeAsync();
        }

        _streams.Clear();

        _ = _connectionHandle.DisposeAsync();
    }
}
