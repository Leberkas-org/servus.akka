using Akka.Actor;
using static Servus.Senf;

namespace Servus.Akka.Transport.Tcp.Client;

internal sealed class TcpConnectionStateMachine(
    IConnectionOperations ops,
    IActorRef connectionManager,
    IPoolingStrategy poolingStrategy,
    IActorRef self)
{
    private const string ConnectTimerKey = "connect-timeout";
    private const int MaxSyncReads = 8;

    private ConnectionLease? _currentLease;
    private bool _leaseReturned;
    private int _connectionGen;
    private ConnectTransport? _pendingConnect;
    private bool _autoReconnect;

    // Buffers pushed before a lease is acquired; drained into the connection's send queue on
    // acquisition. Orphan-disposed on PostStop / reconnect.
    private readonly Queue<WireBuffer> _preConnectWrites = new();

    private long _bytesInFlight;
    private long _highWatermark = 64 * 1024;
    private long _lowWatermark = 32 * 1024;

    private bool _readInProgress;
    private int _syncReadBudget = MaxSyncReads;
    private bool _upstreamFinished;
    private bool _isReconnecting;
    private bool _disconnectedSignaled;
    private CancellationTokenSource? _acquireCts;
    private ReadEventState _readState = new(0);

    private IDuplexConnection? Connection => _currentLease?.Connection;

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
            case ReadCompleted e:
                if (e.Gen == _connectionGen)
                {
                    _readInProgress = false;
                    OnReadCompleted(e.Buffer);
                }
                else
                {
                    // Stale read from a torn-down/reconnected lease: the buffer is OWNED by this
                    // event (rent-and-receive) — dispose or it leaks the pooled array. A stale
                    // event says nothing about the CURRENT gen's read, so _readInProgress is
                    // deliberately left alone.
                    e.Buffer?.Dispose();
                }

                break;
            case ReadFailed e:
                if (e.Gen == _connectionGen)
                {
                    _readInProgress = false;
                    OnReadFailed(e.Error);
                }

                break;
            case SendFlushed e:
                if (e.Gen == _connectionGen)
                {
                    OnSendFlushed(e.Bytes);
                }

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
        if (_currentLease is null)
        {
            ops.OnCompleteStage();
            return;
        }

        TryCompleteAfterUpstreamFinish();
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

        PushDisconnected(DisconnectReason.Timeout);
        ops.OnSignalPullOutbound();
    }

    public void PostStop()
    {
        ops.OnCancelTimer(ConnectTimerKey);
        CleanupTransport();

        while (_preConnectWrites.TryDequeue(out var orphan))
        {
            orphan.Dispose();
        }
    }

    public void RequestRead()
    {
        var connection = Connection;
        if (connection is null || _readInProgress)
        {
            return;
        }

        _readInProgress = true;

        var readTask = connection.ReceiveAsync();

        if (readTask.IsCompletedSuccessfully && _syncReadBudget > 0)
        {
            _syncReadBudget--;
            _readInProgress = false;
            OnReadCompleted(readTask.Result);
            return;
        }

        _syncReadBudget = MaxSyncReads;
        readTask.PipeTo(self, success: _readState.ReadSuccess, failure: _readState.ReadFailure);
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
        var buffer = data.Buffer;
        data.Return();

        if (_currentLease is null)
        {
            _preConnectWrites.Enqueue(buffer);
            ops.OnSignalPullOutbound();
            return;
        }

        if (!Connection!.TryEnqueue(buffer))
        {
            buffer.Dispose();
            OnInboundComplete(DisconnectReason.Error);
            return;
        }

        _bytesInFlight += buffer.Length;

        // Pull the next outbound item only while we are below the high watermark; otherwise pause and
        // wait for SendFlushed to drain us back below the low watermark.
        if (_bytesInFlight < _highWatermark)
        {
            ops.OnSignalPullOutbound();
        }
    }

    private void HandleDisconnectTransport(DisconnectTransport disconnect)
    {
        CleanupTransport();

        // Contract: every DisconnectTransport is answered with a TransportDisconnected, even when
        // no lease was held. Without the echo a consumer that disconnects on a protocol error and
        // then reconnects never learns the transport went down: OnLeaseAcquired only signals
        // TransportConnected after a signaled disconnect, so its buffered replay hangs forever.
        PushDisconnected(disconnect.Reason);
        ops.OnSignalPullOutbound();
    }

    private void OnLeaseAcquired(ConnectionLease lease)
    {
        ops.OnCancelTimer(ConnectTimerKey);

        _pendingConnect = null;
        _connectionGen++;
        _leaseReturned = false;
        _currentLease = lease;
        _bytesInFlight = 0;
        _highWatermark = lease.Options.OutputHighWatermark;
        _lowWatermark = lease.Options.OutputLowWatermark;

        var gen = _connectionGen;
        _readState = new ReadEventState(gen);

        // Assigned once per lease, before the first enqueue: the send loop reports each fully-sent
        // batch back to this actor. Allocation-free per batch — the closure captures only self + gen.
        lease.Connection.OnFlushed = bytes => self.Tell(new SendFlushed(bytes, gen));

        Tracing.For("Connection").Debug(this, "Channel transport ready");

        // Signal TransportConnected for every lease that follows a signaled disconnect, not only
        // explicit reconnects: a consumer whose FIRST acquisition failed (or that disconnected via
        // DisconnectTransport before ever holding a lease) is also waiting in its reconnecting
        // state and would otherwise never replay its buffered requests. The very first clean
        // connect stays silent — consumers treat TransportConnected as "connection restored".
        if (_isReconnecting || _disconnectedSignaled)
        {
            _isReconnecting = false;
            _disconnectedSignaled = false;
            ops.OnPushInbound(new TransportConnected(_currentLease!.Info));
        }

        DrainPreConnectWrites();
        RequestRead();
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
        PushDisconnected(DisconnectReason.Error);
        ops.OnSignalPullOutbound();
    }

    private void OnReadCompleted(WireBuffer? buffer)
    {
        if (buffer is null)
        {
            OnInboundComplete(DisconnectReason.Graceful);
            return;
        }

        ops.OnPushInbound(TransportData.Rent(buffer));
    }

    private void OnReadFailed(Exception ex)
    {
        Tracing.For("Connection").Warning(this, "Read failed: {0}", ex.Message);
        OnInboundComplete(DisconnectReason.Error);
    }

    private void OnSendFlushed(int bytes)
    {
        var before = _bytesInFlight;
        _bytesInFlight -= bytes;

        if (_upstreamFinished)
        {
            if (_bytesInFlight == 0)
            {
                TryCompleteAfterUpstreamFinish();
            }

            return;
        }

        // Resume the outbound pull only on the edge where in-flight bytes just crossed below the low
        // watermark, so a burst of small flushes doesn't fire a pull per batch.
        if (before >= _lowWatermark && _bytesInFlight < _lowWatermark)
        {
            ops.OnSignalPullOutbound();
        }
    }

    private void TryCompleteAfterUpstreamFinish()
    {
        if (!_upstreamFinished || _currentLease is null)
        {
            return;
        }

        if (_preConnectWrites.Count != 0 || _bytesInFlight != 0)
        {
            return;
        }

        _connectionGen++;
        ReturnLeaseToPool(poolingStrategy.OnUpstreamFinish(_currentLease!));
        _currentLease = null;
        ops.OnCompleteStage();
    }

    private void OnInboundComplete(DisconnectReason reason)
    {
        Tracing.For("Connection").Debug(this, "Disconnected: {0}", reason);
        var poolAction = poolingStrategy.OnDisconnect(_currentLease!, reason);

        if (_autoReconnect && _pendingConnect is null && !_upstreamFinished)
        {
            PushDisconnected(DisconnectReason.Transient);
            _isReconnecting = true;

            while (_preConnectWrites.TryDequeue(out var orphan))
            {
                orphan.Dispose();
            }

            _bytesInFlight = 0;
            _leaseReturned = false;
            ReturnLeaseToPool(poolAction);
            _currentLease = null;

            ops.OnSignalPullOutbound();
            return;
        }

        PushDisconnected(reason);

        _bytesInFlight = 0;
        _leaseReturned = false;
        ReturnLeaseToPool(poolAction);
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

    private void PushDisconnected(DisconnectReason reason)
    {
        _disconnectedSignaled = true;
        ops.OnPushInbound(new TransportDisconnected(reason));
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
        _readInProgress = false;
        _readState = new ReadEventState(_connectionGen);
        _bytesInFlight = 0;

        _acquireCts?.Cancel();
        _acquireCts?.Dispose();
        _acquireCts = null;

        if (_currentLease is null) return;
        _leaseReturned = false;
        ReturnLeaseToPool(PoolAction.Dispose);
        _currentLease.Dispose();
        _currentLease = null;
    }

    private void DrainPreConnectWrites()
    {
        while (_preConnectWrites.TryDequeue(out var buffer))
        {
            if (_currentLease is null)
            {
                buffer.Dispose();
                continue;
            }

            if (!Connection!.TryEnqueue(buffer))
            {
                buffer.Dispose();
                OnInboundComplete(DisconnectReason.Error);
                return;
            }

            _bytesInFlight += buffer.Length;
        }

        if (_currentLease is not null && _bytesInFlight < _highWatermark)
        {
            ops.OnSignalPullOutbound();
        }
    }
}
