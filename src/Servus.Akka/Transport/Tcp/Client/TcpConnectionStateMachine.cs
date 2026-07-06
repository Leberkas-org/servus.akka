using System.IO.Pipelines;
using Akka.Actor;
using static Servus.Senf;

namespace Servus.Akka.Transport.Tcp.Client;

public sealed class TcpConnectionStateMachine(
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

    private readonly Queue<TransportBuffer> _pendingWrites = new();
    private bool _needsFlush;
    private bool _flushInProgress;

    private bool _readInProgress;
    private int _syncReadBudget = MaxSyncReads;
    private bool _upstreamFinished;
    private bool _isReconnecting;
    private bool _disconnectedSignaled;
    private CancellationTokenSource? _acquireCts;

    private SocketPipeConnection? Connection => _currentLease?.Connection;

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
            case PipeFlushComplete e:
                if (e.Gen == _connectionGen)
                {
                    _flushInProgress = false;
                    FlushPendingWrites();
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
        }
        else if (_pendingWrites.Count == 0 && !_flushInProgress)
        {
            FlushIfNeeded();
            _connectionGen++;
            ReturnLeaseToPool(poolingStrategy.OnUpstreamFinish(_currentLease!));
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

        PushDisconnected(DisconnectReason.Timeout);
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

    public void RequestRead()
    {
        var connection = Connection;
        if (connection is null || _readInProgress)
        {
            return;
        }

        if (!_flushInProgress)
        {
            FlushIfNeeded();
        }

        _readInProgress = true;

        var gen = _connectionGen;
        var readTask = connection.ReceiveAsync();

        if (readTask.IsCompletedSuccessfully && _syncReadBudget > 0)
        {
            _syncReadBudget--;
            _readInProgress = false;
            OnReadCompleted(readTask.Result);
            return;
        }

        _syncReadBudget = MaxSyncReads;
        readTask.PipeTo(self,
            success: buffer => new ReadCompleted(buffer, gen),
            failure: ex => new ReadFailed(ex, gen));
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
        if (_currentLease is null || _flushInProgress)
        {
            _pendingWrites.Enqueue(data.Buffer);
            ops.OnSignalPullOutbound();
            return;
        }

        if (!WriteToOutputPipe(data.Buffer))
        {
            OnInboundComplete(DisconnectReason.Error);
            return;
        }

        FlushIfNeeded();

        if (!_flushInProgress)
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

        Tracing.For("Connection").Debug(this, "Pipe transport ready");

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

        FlushPendingWrites();
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

    private void OnReadCompleted(TransportBuffer? buffer)
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

    private void OnInboundComplete(DisconnectReason reason)
    {
        Tracing.For("Connection").Debug(this, "Disconnected: {0}", reason);
        var poolAction = poolingStrategy.OnDisconnect(_currentLease!, reason);

        if (_autoReconnect && _pendingConnect is null && !_upstreamFinished)
        {
            PushDisconnected(DisconnectReason.Transient);
            _isReconnecting = true;

            while (_pendingWrites.TryDequeue(out var orphan))
            {
                orphan.Dispose();
            }

            _leaseReturned = false;
            ReturnLeaseToPool(poolAction);
            _currentLease = null;

            ops.OnSignalPullOutbound();
            return;
        }

        PushDisconnected(reason);

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

        _acquireCts?.Cancel();
        _acquireCts?.Dispose();
        _acquireCts = null;

        if (_currentLease is null) return;
        _leaseReturned = false;
        ReturnLeaseToPool(PoolAction.Dispose);
        _currentLease.Dispose();
        _currentLease = null;
    }

    private void FlushPendingWrites()
    {
        while (_pendingWrites.TryDequeue(out var buffer))
        {
            if (_currentLease is not null)
            {
                if (!WriteToOutputPipe(buffer))
                {
                    OnInboundComplete(DisconnectReason.Error);
                    return;
                }
            }
            else
            {
                buffer.Dispose();
            }
        }

        FlushIfNeeded();

        if (!_flushInProgress)
        {
            ops.OnSignalPullOutbound();
        }
    }

    private bool WriteToOutputPipe(TransportBuffer data)
    {
        try
        {
            var writer = _currentLease!.OutputWriter;
            var mem = writer.GetMemory(data.Length);
            data.Memory.Span.CopyTo(mem.Span);
            writer.Advance(data.Length);
            data.Dispose();
            _needsFlush = true;
            return true;
        }
        catch (InvalidOperationException)
        {
            data.Dispose();
            return false;
        }
    }

    private void FlushIfNeeded()
    {
        if (!_needsFlush || _currentLease is null)
        {
            return;
        }

        _needsFlush = false;
        var gen = _connectionGen;

        ValueTask<FlushResult> flush;
        try
        {
            flush = _currentLease.OutputWriter.FlushAsync();
        }
        catch (InvalidOperationException)
        {
            return;
        }

        if (flush.IsCompleted)
        {
            return;
        }

        _flushInProgress = true;
        flush.PipeTo(self,
            success: _ => new PipeFlushComplete(gen),
            failure: _ => new PipeFlushComplete(gen));
    }
}