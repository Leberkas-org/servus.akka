using System.Net;
using Akka.Actor;
using static Servus.Senf;

namespace Servus.Akka.Transport.Quic.Client;

internal sealed class QuicTransportStateMachine : IQuicStreamReadHost
{
    private const string ConnectTimerKey = "connect-timeout";
    private const string MigrationCheckTimerKey = "migration-check";
    private const int MaxSyncReads = 8;
    private static readonly TimeSpan MigrationCheckInterval = TimeSpan.FromSeconds(5);

    private readonly IConnectionOperations _ops;
    private readonly IActorRef _connectionManager;
    private readonly IActorRef _self;
    private readonly QuicStreamLifecycle _lifecycle;

    private QuicConnectionHandle? _connectionHandle;
    private QuicConnectionLease? _connectionLease;
    private ConnectTransport? _pendingConnect;
    private bool _autoReconnect;
    private TransportConnectionOptions? _transportOptions;
    private bool _upstreamFinished;
    private bool _isReconnecting;
    private CancellationTokenSource? _acquireCts;
    private CancellationTokenSource? _acceptCts;
    private EndPoint? _lastRemoteEndPoint;

    public QuicTransportStateMachine(
        IConnectionOperations ops, IActorRef connectionManager, IActorRef self)
    {
        _ops = ops;
        _connectionManager = connectionManager;
        _self = self;
        _lifecycle = new QuicStreamLifecycle(ops, self, this, MaxSyncReads);
    }

    IConnectionOperations IQuicStreamReadHost.Ops => _ops;

    bool IQuicStreamReadHost.TryGetStream(StreamTarget id, out QuicStreamState state)
        => _lifecycle.TryGetStream(id, out state);

    void IQuicStreamReadHost.RequestStreamRead(StreamTarget streamId)
        => _lifecycle.RequestStreamRead(streamId);

    void IQuicStreamReadHost.OnInboundComplete(DisconnectReason reason, long rawStreamId)
        => _lifecycle.OnInboundComplete(reason, rawStreamId);

    void IQuicStreamReadHost.OnReadFailure(QuicStreamState state, Exception error)
    {
        if (IsConnectionLevelError(error))
        {
            HandleConnectionFailure(DisconnectReason.Error);
        }
        else
        {
            _lifecycle.OnInboundComplete(DisconnectReason.Error, state.StreamId);
        }
    }

    internal void Dispatch(IQuicTransportEvent evt)
    {
        switch (evt)
        {
            case ConnectionLeaseAcquired e:
                OnConnectionLeaseAcquired(e.Lease);
                break;
            case StreamLeaseAcquired e:
                _lifecycle.OnStreamLeaseAcquired(e.Stream, e.StreamId);
                break;
            case AcquisitionFailed e:
                OnAcquisitionFailed(e.Error);
                break;
            case StreamReceiveCompleted e:
                QuicStreamReads.OnReceiveCompleted(this, e.State, e.Buffer, e.Epoch);
                break;
            case StreamReceiveFailed e:
                QuicStreamReads.OnReceiveFailed(this, e.State, e.Error, e.Epoch);
                break;
            case AcceptLoopFailed e:
                Tracing.For("Connection").Warning(this, "QUIC accept loop failed: {0}", e.Error.Message);
                HandleConnectionFailure(DisconnectReason.Error);
                break;
            case InboundStreamAccepted e:
                _lifecycle.OnInboundStreamAccepted(e.Stream, e.StreamId, _transportOptions);
                break;
            case MigrationDetected e:
                _ops.OnPushInbound(new ConnectionMigrationDetected(e.OldEndPoint, e.NewEndPoint));
                break;
            case StreamSendFlushed e:
                _lifecycle.OnStreamSendFlushed(e.StreamId, e.Bytes, e.Epoch);
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
                if (_connectionHandle is not null)
                {
                    _lifecycle.HandleOpenStream(open.StreamId, open.Direction, _connectionHandle, _transportOptions);
                }
                else
                {
                    _ops.OnSignalPullOutbound();
                }

                break;
            case MultiplexedData data:
                _lifecycle.HandleMultiplexedData(data);
                break;
            case CompleteWrites cw:
                _lifecycle.HandleCompleteWrites(cw.StreamId);
                break;
            case ResetStream rs:
                _lifecycle.HandleResetStream(rs.StreamId, rs.ErrorCode);
                break;
            case DisconnectTransport:
                CleanupTransport();
                _ops.OnSignalPullOutbound();
                break;
        }
    }

    public void HandleUpstreamFinish()
    {
        _upstreamFinished = true;
        CleanupTransport();
        _ops.OnCompleteStage();
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
            _ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);
            return;
        }

        if (timerKey != ConnectTimerKey || _pendingConnect is null)
        {
            return;
        }

        _pendingConnect = null;
        _ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Timeout));
        _ops.OnSignalPullOutbound();
    }

    public void PostStop()
    {
        _ops.OnCancelTimer(ConnectTimerKey);
        _ops.OnCancelTimer(MigrationCheckTimerKey);
        CleanupTransport();
    }

    internal void NotifyItemDelivered(StreamTarget streamId) => _lifecycle.NotifyItemDelivered(streamId);

    internal QuicStreamState RegisterTestStream(long streamId, StreamDirection direction)
        => _lifecycle.RegisterTestStream(streamId, direction, _transportOptions);

    private void HandleConnectTransport(ConnectTransport connect)
    {
        if (connect.Options is QuicTransportOptions quicOpts)
        {
            _autoReconnect = quicOpts.AutoReconnect;
        }

        _transportOptions = TransportConnectionOptions.FromQuicTransport(connect.Options);

        if (_connectionLease is not null)
        {
            _isReconnecting = true;
        }

        CleanupTransport();
        _pendingConnect = connect;
        AcquireConnection(connect);
        _ops.OnSignalPullOutbound();
    }

    private void OnConnectionLeaseAcquired(QuicConnectionLease lease)
    {
        _ops.OnCancelTimer(ConnectTimerKey);
        _pendingConnect = null;
        _connectionLease = lease;
        _connectionHandle = lease.Handle;
        _lastRemoteEndPoint = _connectionHandle.RemoteEndPoint();
        _ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);

        StartAcceptLoop(_connectionHandle);
        Tracing.For("Connection").Debug(this, "QUIC transport ready");

        if (_isReconnecting)
        {
            _isReconnecting = false;
        }

        var info = new ConnectionInfo(
            _connectionHandle.LocalEndPoint()!,
            _connectionHandle.RemoteEndPoint()!,
            TransportProtocol.Quic);
        _ops.OnPushInbound(new TransportConnected(info));
    }

    private void OnAcquisitionFailed(Exception ex)
    {
        if (ex is OperationCanceledException)
        {
            return;
        }

        _ops.OnCancelTimer(ConnectTimerKey);
        Tracing.For("Connection").Warning(this, "QUIC acquisition failed: {0}", ex.Message);

        if (_pendingConnect is not null)
        {
            _pendingConnect = null;
            _ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Error));
            _ops.OnSignalPullOutbound();
            return;
        }

        HandleConnectionFailure(DisconnectReason.Error);
    }

    private void HandleConnectionFailure(DisconnectReason reason)
    {
        Tracing.For("Connection").Debug(this, "QUIC disconnected: {0}", reason);

        if (_autoReconnect && !_upstreamFinished)
        {
            _lifecycle.DisposeAllStreams();
            _ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Transient));
            _isReconnecting = true;
            StopAcceptLoop();
            ReturnConnectionToPool(false);
            _connectionHandle = null;
            _connectionLease = null;
            _ops.OnSignalPullOutbound();
            return;
        }

        _lifecycle.DisposeAllStreamsWithNotification(reason);
        _ops.OnPushInbound(new TransportDisconnected(reason));
        StopAcceptLoop();
        ReturnConnectionToPool(false);
        _connectionHandle = null;
        _connectionLease = null;

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
        if (ConnectionMigrationCheck.TryDetect(
                _connectionHandle?.RemoteEndPoint(), ref _lastRemoteEndPoint, out var old, out var current))
        {
            _ops.OnPushInbound(new ConnectionMigrationDetected(old, current));
        }
    }

    private void AcquireConnection(ConnectTransport connect)
    {
        _acquireCts?.Cancel();
        _acquireCts?.Dispose();
        _acquireCts = new CancellationTokenSource();

        if (connect.Options is QuicTransportOptions quicOpts)
        {
            QuicConnectionManagerActor.AcquireAsync(_connectionManager, quicOpts, _acquireCts.Token)
                .PipeTo(_self,
                    success: lease => new ConnectionLeaseAcquired(lease),
                    failure: ex => new AcquisitionFailed(ex));
        }

        var timeout = connect.Options.ConnectTimeout;
        if (timeout <= TimeSpan.Zero)
        {
            timeout = TimeSpan.FromSeconds(10);
        }

        _ops.OnScheduleTimer(ConnectTimerKey, timeout);
    }

    private void StartAcceptLoop(QuicConnectionHandle connectionHandle)
    {
        _acceptCts?.Cancel();
        _acceptCts?.Dispose();
        _acceptCts = new CancellationTokenSource();
        _ = AcceptLoopAsync(connectionHandle, _self, _acceptCts.Token);
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
        try
        {
            while (!ct.IsCancellationRequested)
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
                    return;
                }

                self.Tell(new InboundStreamAccepted(result.Value.Stream, result.Value.StreamId));
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Graceful shutdown via StopAcceptLoop — not a failure.
        }
        catch (Exception ex)
        {
            // Never let a connection-level fault become an unobserved task exception: route it to the
            // actor so it surfaces as a connection failure (and reconnect) instead of silently dying.
            self.Tell(new AcceptLoopFailed(ex));
        }
    }

    private void ReturnConnectionToPool(bool canReuse)
    {
        if (_connectionLease is null)
        {
            return;
        }

        var lease = _connectionLease;
        _connectionLease = null;

        _connectionManager.Tell(new QuicConnectionManagerActor.Release(lease, canReuse));

        if (!canReuse)
        {
            _ = lease.DisposeAsync();
        }
    }

    private void CleanupTransport()
    {
        StopAcceptLoop();

        _acquireCts?.Cancel();
        _acquireCts?.Dispose();
        _acquireCts = null;

        _lifecycle.DisposeAllStreams();

        ReturnConnectionToPool(false);
        _connectionHandle = null;
        _connectionLease = null;
    }

    private static bool IsConnectionLevelError(Exception ex) => QuicErrorClassifier.IsConnectionLevel(ex);
}

#pragma warning restore CA1416
