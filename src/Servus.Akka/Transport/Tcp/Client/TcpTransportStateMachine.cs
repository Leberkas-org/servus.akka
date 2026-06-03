using System.Buffers;
using Akka.Actor;
using static Servus.Senf;

namespace Servus.Akka.Transport.Tcp.Client;

public sealed class TcpTransportStateMachine(
    ITransportOperations ops,
    IActorRef connectionManager,
    IPoolingStrategy poolingStrategy,
    IActorRef self)
{
    private const string ConnectTimerKey = "connect-timeout";

    private ConnectionHandle? _handle;
    private ConnectionLease? _currentLease;
    private bool _leaseReturned;
    private int _connectionGen;
    private ConnectTransport? _pendingConnect;
    private bool _autoReconnect;

    private readonly Queue<TransportBuffer> _pendingWrites = new();

    private bool _upstreamFinished;
    private bool _isReconnecting;
    private TcpPumpManager? _pumpManager;
    private CancellationTokenSource? _acquireCts;

    internal void Dispatch(ITcpTransportEvent evt)
    {
        switch (evt)
        {
            case LeaseAcquired e:
                OnLeaseAcquired(e.Lease);
                break;
            case AcquisitionFailed e:
                OnAcquisitionFailed(e.Error);
                break;
            case InboundBatch e:
                if (e.Gen == _connectionGen)
                {
                    OnInboundBatch(e.Batch, e.Count);
                }
                else
                {
                    ArrayPool<ITransportInbound>.Shared.Return(e.Batch);
                }
                break;
            case InboundComplete e:
                if (e.Gen == _connectionGen)
                {
                    OnInboundComplete(e.Reason);
                }
                break;
            case InboundPumpFailed:
                OnInboundComplete(DisconnectReason.Error);
                break;
            case OutboundWriteDone:
                break;
            case OutboundWriteFailed e:
                OnOutboundWriteFailed(e.Error);
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
            case TransportData data:
                HandleTransportData(data);
                break;
            case DisconnectTransport disconnect:
                HandleDisconnectTransport(disconnect);
                break;
        }
    }

    public void HandleUpstreamFinish()
    {
        _upstreamFinished = true;
        if (_handle is null)
        {
            ops.OnCompleteStage();
        }
        else if (_pendingWrites.Count == 0)
        {
            _connectionGen++;
            _pumpManager?.StopPumps();
            ReturnLeaseToPool(poolingStrategy.OnUpstreamFinish(_currentLease!));
            _handle = null;
            _currentLease = null;
            ops.OnCompleteStage();
        }
    }

    public void HandleDownstreamFinish()
    {
        CleanupTransport();
    }

    public void OnTimer(string? timerKey)
    {
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
        CleanupTransport();

        while (_pendingWrites.TryDequeue(out var orphan))
        {
            orphan.Dispose();
        }
    }

    private void HandleConnectTransport(ConnectTransport connect)
    {
        if (connect.Options is TcpTransportOptions tcpOpts)
        {
            _autoReconnect = tcpOpts.AutoReconnect;
        }

        if (_currentLease is not null)
        {
            _isReconnecting = true;
        }

        CleanupTransport();
        _pendingConnect = connect;
        AcquireConnection(connect);
        ops.OnSignalPullOutbound();
    }

    private void HandleTransportData(TransportData data)
    {
        if (_handle is null)
        {
            _pendingWrites.Enqueue(data.Buffer);
            ops.OnSignalPullOutbound();
            return;
        }

        _handle.Write(data.Buffer);
        ops.OnSignalPullOutbound();
    }

    private void HandleDisconnectTransport(DisconnectTransport disconnect)
    {
        CleanupTransport();
        ops.OnSignalPullOutbound();
    }

    private void OnLeaseAcquired(ConnectionLease lease)
    {
        ops.OnCancelTimer(ConnectTimerKey);

        _pendingConnect = null;
        _connectionGen++;
        _leaseReturned = false;
        _currentLease = lease;
        _handle = lease.Handle;

        _pumpManager = new TcpPumpManager(self);
        _pumpManager.StartPumps(lease.State, _connectionGen);
        Tracing.For("Connection").Debug(this, "Transport ready");

        if (_isReconnecting)
        {
            _isReconnecting = false;
            ops.OnPushInbound(new TransportConnected(_currentLease!.Info));
        }

        FlushPendingWrites();
    }

    private void OnAcquisitionFailed(Exception ex)
    {
        if (ex is OperationCanceledException)
        {
            return;
        }

        ops.OnCancelTimer(ConnectTimerKey);
        Tracing.For("Connection").Warning(this, "Acquisition failed: {0}", ex.Message);

        if (_pendingConnect is null)
        {
            return;
        }

        _pendingConnect = null;
        ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Error));
        ops.OnSignalPullOutbound();
    }

    private void OnInboundBatch(ITransportInbound[] batch, int count)
    {
        for (var i = 0; i < count; i++)
        {
            ops.OnPushInbound(batch[i]);
            batch[i] = null!;
        }

        ArrayPool<ITransportInbound>.Shared.Return(batch);
    }

    private void OnInboundComplete(DisconnectReason reason)
    {
        Tracing.For("Connection").Debug(this, "Disconnected: {0}", reason);
        var poolAction = poolingStrategy.OnDisconnect(_currentLease!, reason);

        if (_autoReconnect && _pendingConnect is null && !_upstreamFinished)
        {
            ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Transient));
            _isReconnecting = true;

            while (_pendingWrites.TryDequeue(out var orphan))
            {
                orphan.Dispose();
            }

            _leaseReturned = false;
            ReturnLeaseToPool(poolAction);
            _handle = null;
            _currentLease = null;

            ops.OnSignalPullOutbound();
            return;
        }

        ops.OnPushInbound(new TransportDisconnected(reason));

        _leaseReturned = false;
        ReturnLeaseToPool(poolAction);
        _pumpManager?.StopPumps();
        _handle = null;
        _currentLease = null;

        if (_upstreamFinished)
        {
            ops.OnCompleteStage();
        }
        else
        {
            ops.OnSignalPullOutbound();
        }
    }

    private void OnOutboundWriteFailed(Exception ex)
    {
        Tracing.For("Connection").Warning(this, "Write failed: {0}", ex.Message);
        _leaseReturned = false;
        ReturnLeaseToPool(poolingStrategy.OnDisconnect(_currentLease!, DisconnectReason.Error));

        ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Error));
        _pumpManager?.StopPumps();
        _handle = null;
        _currentLease = null;
        ops.OnSignalPullOutbound();
    }

    private void AcquireConnection(ConnectTransport connect)
    {
        _acquireCts?.Cancel();
        _acquireCts?.Dispose();
        _acquireCts = new CancellationTokenSource();

        TcpConnectionManagerActor.AcquireAsync(connectionManager, connect.Options, _acquireCts.Token)
            .PipeTo(self,
                success: lease => new LeaseAcquired(lease),
                failure: ex => new AcquisitionFailed(ex));

        var timeout = connect.Options.ConnectTimeout;
        if (timeout <= TimeSpan.Zero)
        {
            timeout = TimeSpan.FromSeconds(10);
        }

        ops.OnScheduleTimer(ConnectTimerKey, timeout);
    }

    private void ReturnLeaseToPool(PoolAction action)
    {
        if (_leaseReturned || _currentLease is null)
        {
            return;
        }

        _leaseReturned = true;
        var canReuse = action == PoolAction.Reuse;
        connectionManager.Tell(new TcpConnectionManagerActor.Release(_currentLease, canReuse));
    }

    private void CleanupTransport()
    {
        _connectionGen++;
        _pumpManager?.StopPumps();

        _acquireCts?.Cancel();
        _acquireCts?.Dispose();
        _acquireCts = null;

        if (_currentLease is not null)
        {
            _leaseReturned = false;
            ReturnLeaseToPool(PoolAction.Dispose);
            _currentLease.Dispose();
            _currentLease = null;
            _handle = null;
        }
    }

    private void FlushPendingWrites()
    {
        while (_pendingWrites.TryDequeue(out var buffer))
        {
            if (_handle is not null)
            {
                _handle.Write(buffer);
            }
            else
            {
                buffer.Dispose();
            }
        }

        ops.OnSignalPullOutbound();
    }
}
