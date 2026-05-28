using System.Net;
using Akka.Actor;
using static Servus.Core.Servus;

namespace Servus.Akka.Transport.Quic.Client;

public sealed class QuicTransportStateMachine(
    ITransportOperations ops,
    IActorRef connectionManager,
    IActorRef self)
{
    private const string ConnectTimerKey = "connect-timeout";
    private const string MigrationCheckTimerKey = "migration-check";
    private static readonly TimeSpan MigrationCheckInterval = TimeSpan.FromSeconds(5);

    private QuicConnectionHandle? _connectionHandle;
    private QuicConnectionLease? _connectionLease;
    private int _connectionGen;
    private ConnectTransport? _pendingConnect;
    private bool _autoReconnect;
    private bool _upstreamFinished;
    private bool _isReconnecting;
    private CancellationTokenSource? _acquireCts;
    private EndPoint? _lastRemoteEndPoint;

    private readonly Dictionary<StreamTarget, QuicStreamState> _streams = new();
    private QuicPumpManager? _pumpManager;

    internal void Dispatch(IQuicTransportEvent evt)
    {
        switch (evt)
        {
            case ConnectionLeaseAcquired e:
                OnConnectionLeaseAcquired(e.Lease);
                break;
            case StreamLeaseAcquired e:
                OnStreamLeaseAcquired(e.Handle, e.StreamId);
                break;
            case AcquisitionFailed e:
                OnAcquisitionFailed(e.Error);
                break;
            case InboundData e:
                if (e.Gen == _connectionGen)
                {
                    ops.OnPushInbound(new MultiplexedData(e.Buffer, StreamTarget.FromId(e.StreamId)));
                }
                else
                {
                    e.Buffer.Dispose();
                }

                break;
            case InboundStreamAccepted e:
                OnInboundStreamAccepted(e.Stream, e.StreamId);
                break;
            case InboundComplete e:
                if (e.Gen == _connectionGen)
                {
                    OnInboundComplete(e.Reason, e.StreamId);
                }

                break;
            case InboundPumpFailed e:
                if (IsConnectionLevelError(e.Error))
                {
                    HandleConnectionFailure(DisconnectReason.Error);
                }
                else
                {
                    OnInboundComplete(DisconnectReason.Error, e.StreamId);
                }

                break;
            case OutboundWriteDone:
                ops.OnSignalPullOutbound();
                break;
            case OutboundWriteFailed e:
                OnOutboundWriteFailed(e.Error);
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
            case ConnectTransport connect:
                HandleConnectTransport(connect);
                break;
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
                CleanupTransport();
                ops.OnSignalPullOutbound();
                break;
        }
    }

    public void HandleUpstreamFinish()
    {
        _upstreamFinished = true;
        if (_connectionHandle is null)
        {
            ops.OnCompleteStage();
            return;
        }

        _pumpManager?.StopAll();
        ops.OnCompleteStage();
    }

    public void HandleDownstreamFinish()
    {
        CleanupTransport();
    }

    public void OnTimer(string? timerKey)
    {
        if (timerKey == MigrationCheckTimerKey)
        {
            CheckForConnectionMigration();
            ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);
            return;
        }

        if (timerKey != ConnectTimerKey || _pendingConnect is null)
        {
            return;
        }

        _pendingConnect = null;

        ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Timeout));
        ops.OnSignalPullOutbound();
    }

    public void PostStop()
    {
        ops.OnCancelTimer(ConnectTimerKey);
        ops.OnCancelTimer(MigrationCheckTimerKey);
        CleanupTransport();
    }

    private void HandleConnectTransport(ConnectTransport connect)
    {
        if (connect.Options is QuicTransportOptions quicOpts)
        {
            _autoReconnect = quicOpts.AutoReconnect;
        }

        if (_connectionLease is not null)
        {
            _isReconnecting = true;
        }

        CleanupTransport();
        _pendingConnect = connect;
        AcquireConnection(connect);
        ops.OnSignalPullOutbound();
    }

    private void HandleOpenStream(StreamTarget streamId, StreamDirection direction)
    {
        if (_connectionHandle is null)
        {
            ops.OnSignalPullOutbound();
            return;
        }

        var state = new QuicStreamState(direction);
        _streams[streamId] = state;

        var sid = streamId.Value;
        _connectionHandle.OpenStreamAsync(direction)
            .PipeTo(self,
                success: result => new StreamLeaseAcquired(new StreamHandle(result.Stream), sid),
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

        ops.OnSignalPullOutbound();
    }

    private void HandleCompleteWrites(StreamTarget streamId)
    {
        if (_streams.TryGetValue(streamId, out var state))
        {
            state.CompleteWrites();
        }

        ops.OnSignalPullOutbound();
    }

    private void HandleResetStream(StreamTarget streamId, long errorCode)
    {
        if (_streams.Remove(streamId, out var state))
        {
            state.Abort(errorCode);
            _ = state.DisposeAsync();
            ops.OnPushInbound(new StreamClosed(streamId, DisconnectReason.Error));
        }

        ops.OnSignalPullOutbound();
    }

    private void OnConnectionLeaseAcquired(QuicConnectionLease lease)
    {
        ops.OnCancelTimer(ConnectTimerKey);
        _pendingConnect = null;
        _connectionGen++;
        _connectionLease = lease;
        _connectionHandle = lease.Handle;
        _lastRemoteEndPoint = _connectionHandle.RemoteEndPoint();
        ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);

        _pumpManager = new QuicPumpManager(self);
        _pumpManager.StartAcceptLoop(_connectionHandle);
        Tracing.For("Connection").Debug(this, "QUIC transport ready");

        if (_isReconnecting)
        {
            _isReconnecting = false;
        }

        var info = new ConnectionInfo(
            _connectionHandle.LocalEndPoint()!,
            _connectionHandle.RemoteEndPoint()!,
            TransportProtocol.Quic);
        ops.OnPushInbound(new TransportConnected(info));
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
        if (state.Direction == StreamDirection.Bidirectional)
        {
            _pumpManager?.StartInboundPump(handle, rawStreamId, _connectionGen);
        }

        ops.OnPushInbound(new StreamOpened(streamId, state.Direction));
    }

    private void OnInboundStreamAccepted(Stream stream, long rawStreamId)
    {
        var streamId = StreamTarget.FromId(rawStreamId);
        var handle = new StreamHandle(stream);
        var state = new QuicStreamState(StreamDirection.Unidirectional);
        state.AttachHandle(handle);
        _streams[streamId] = state;

        _pumpManager?.StartInboundPump(handle, rawStreamId, _connectionGen);
        ops.OnPushInbound(new ServerStreamAccepted(streamId, StreamDirection.Unidirectional));
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

            ops.OnPushInbound(new StreamReadCompleted(streamId));
        }
        else
        {
            _streams.Remove(streamId);
            _ = state.DisposeAsync();
            ops.OnPushInbound(new StreamClosed(streamId, reason));
        }
    }

    private void OnOutboundWriteFailed(Exception ex)
    {
        Tracing.For("Connection").Warning(this, "QUIC write failed: {0}", ex.Message);
        HandleConnectionFailure(DisconnectReason.Error);
    }

    private void OnAcquisitionFailed(Exception ex)
    {
        if (ex is OperationCanceledException)
        {
            return;
        }

        ops.OnCancelTimer(ConnectTimerKey);
        Tracing.For("Connection").Warning(this, "QUIC acquisition failed: {0}", ex.Message);

        if (_pendingConnect is not null)
        {
            _pendingConnect = null;
            ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Error));
            ops.OnSignalPullOutbound();
            return;
        }

        HandleConnectionFailure(DisconnectReason.Error);
    }

    private void HandleConnectionFailure(DisconnectReason reason)
    {
        Tracing.For("Connection").Debug(this, "QUIC disconnected: {0}", reason);

        if (_autoReconnect && !_upstreamFinished)
        {
            foreach (var (_, state) in _streams)
            {
                _ = state.DisposeAsync();
            }

            _streams.Clear();

            ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Transient));
            _isReconnecting = true;
            _pumpManager?.StopAll();
            ReturnConnectionToPool(false);
            _connectionHandle = null;
            _connectionLease = null;
            ops.OnSignalPullOutbound();
            return;
        }

        foreach (var (target, state) in _streams)
        {
            ops.OnPushInbound(new StreamClosed(target, reason));
            _ = state.DisposeAsync();
        }

        _streams.Clear();

        ops.OnPushInbound(new TransportDisconnected(reason));
        _pumpManager?.StopAll();
        ReturnConnectionToPool(false);
        _connectionHandle = null;
        _connectionLease = null;

        if (_upstreamFinished)
        {
            ops.OnCompleteStage();
        }
        else
        {
            ops.OnSignalPullOutbound();
        }
    }

    private void CheckForConnectionMigration()
    {
        var currentRemote = _connectionHandle?.RemoteEndPoint();
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

    private void AcquireConnection(ConnectTransport connect)
    {
        _acquireCts?.Cancel();
        _acquireCts?.Dispose();
        _acquireCts = new CancellationTokenSource();

        if (connect.Options is QuicTransportOptions quicOpts)
        {
            QuicConnectionManagerActor.AcquireAsync(connectionManager, quicOpts, _acquireCts.Token)
                .PipeTo(self,
                    success: lease => new ConnectionLeaseAcquired(lease),
                    failure: ex => new AcquisitionFailed(ex));
        }

        var timeout = connect.Options.ConnectTimeout;
        if (timeout <= TimeSpan.Zero)
        {
            timeout = TimeSpan.FromSeconds(10);
        }

        ops.OnScheduleTimer(ConnectTimerKey, timeout);
    }

    private void ReturnConnectionToPool(bool canReuse)
    {
        if (_connectionLease is null)
        {
            return;
        }

        var lease = _connectionLease;
        _connectionLease = null;

        connectionManager.Tell(new QuicConnectionManagerActor.Release(lease, canReuse));

        if (!canReuse)
        {
            _ = lease.DisposeAsync();
        }
    }

    private void CleanupTransport()
    {
        _connectionGen++;
        _pumpManager?.StopAll();

        _acquireCts?.Cancel();
        _acquireCts?.Dispose();
        _acquireCts = null;

        foreach (var (_, state) in _streams)
        {
            _ = state.DisposeAsync();
        }

        _streams.Clear();

        ReturnConnectionToPool(false);
        _connectionHandle = null;
        _connectionLease = null;
    }
    private static bool IsConnectionLevelError(Exception ex)
    {
        if (ex is System.Net.Quic.QuicException qe)
        {
            return qe.QuicError is System.Net.Quic.QuicError.ConnectionAborted
                or System.Net.Quic.QuicError.ConnectionIdle
                or System.Net.Quic.QuicError.ConnectionRefused
                or System.Net.Quic.QuicError.ConnectionTimeout;
        }

        return ex is ObjectDisposedException;
    }
}

#pragma warning restore CA1416