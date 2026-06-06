using System.Buffers;
using System.IO.Pipelines;
using Akka.Actor;
using static Servus.Senf;

namespace Servus.Akka.Transport.Tcp.Client;

public sealed class PipeTransportStateMachine(
    ITransportOperations ops,
    IActorRef connectionManager,
    IPoolingStrategy poolingStrategy,
    IActorRef self)
{
    private const string ConnectTimerKey = "connect-timeout";

    private SocketPipeConnection? _connection;
    private ConnectionLease? _currentLease;
    private bool _leaseReturned;
    private int _connectionGen;
    private ConnectTransport? _pendingConnect;
    private bool _autoReconnect;

    private readonly Queue<TransportBuffer> _pendingWrites = new();
    private readonly LeaseTracker _leaseTracker = new(16);

    private SequencePosition? _pendingAdvance;
    private bool _upstreamFinished;
    private bool _isReconnecting;
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
            case PipeReadComplete e:
                if (e.Gen == _connectionGen)
                {
                    OnPipeReadComplete(e.Result);
                }
                break;
            case PipeReadFailed e:
                if (e.Gen == _connectionGen)
                {
                    OnPipeReadFailed(e.Error);
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
        if (_connection is null)
        {
            ops.OnCompleteStage();
        }
        else if (_pendingWrites.Count == 0)
        {
            _connectionGen++;
            DisposeConnection();
            _leaseTracker.ForceReturnAll();
            ReturnLeaseToPool(poolingStrategy.OnUpstreamFinish(_currentLease!));
            _connection = null;
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
        _leaseTracker.ForceReturnAll();
        CleanupTransport();

        while (_pendingWrites.TryDequeue(out var orphan))
        {
            orphan.Dispose();
        }
    }

    public void RequestRead()
    {
        if (_connection is null)
        {
            return;
        }

        if (_pendingAdvance is { } pos)
        {
            _pendingAdvance = null;
            _connection.InputReader.AdvanceTo(pos);
        }

        var gen = _connectionGen;
        _connection.InputReader.ReadAsync().AsTask().PipeTo(self,
            success: result => new PipeReadComplete(result, gen),
            failure: ex => new PipeReadFailed(ex, gen));
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
        if (_connection is null)
        {
            _pendingWrites.Enqueue(data.Buffer);
            ops.OnSignalPullOutbound();
            return;
        }

        WriteToOutputPipe(data.Buffer);
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
        _connection = SocketPipeConnection.Create(lease.State.Stream);

        Tracing.For("Connection").Debug(this, "Pipe transport ready");

        if (_isReconnecting)
        {
            _isReconnecting = false;
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
        ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Error));
        ops.OnSignalPullOutbound();
    }

    private void OnPipeReadComplete(ReadResult result)
    {
        if (result.Buffer.Length > 0)
        {
            var length = (int)result.Buffer.Length;
            var buf = TransportBuffer.Rent(length);
            result.Buffer.CopyTo(buf.FullMemory.Span);
            buf.Length = length;
            _pendingAdvance = result.Buffer.End;
            ops.OnPushInbound(new TransportData(buf));
        }

        if (result.IsCompleted || result.IsCanceled)
        {
            OnInboundComplete(DisconnectReason.Graceful);
        }
    }

    private void OnPipeReadFailed(Exception ex)
    {
        Tracing.For("Connection").Warning(this, "Pipe read failed: {0}", ex.Message);
        OnInboundComplete(DisconnectReason.Error);
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
            _leaseTracker.ForceReturnAll();
            ReturnLeaseToPool(poolAction);
            DisposeConnection();
            _connection = null;
            _currentLease = null;

            ops.OnSignalPullOutbound();
            return;
        }

        ops.OnPushInbound(new TransportDisconnected(reason));

        _leaseReturned = false;
        _leaseTracker.ForceReturnAll();
        ReturnLeaseToPool(poolAction);
        DisposeConnection();
        _connection = null;
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
        _pendingAdvance = null;
        _leaseTracker.ForceReturnAll();
        DisposeConnection();

        _acquireCts?.Cancel();
        _acquireCts?.Dispose();
        _acquireCts = null;

        if (_currentLease is not null)
        {
            _leaseReturned = false;
            ReturnLeaseToPool(PoolAction.Dispose);
            _currentLease.Dispose();
            _currentLease = null;
            _connection = null;
        }
    }

    private void FlushPendingWrites()
    {
        while (_pendingWrites.TryDequeue(out var buffer))
        {
            if (_connection is not null)
            {
                WriteToOutputPipe(buffer);
            }
            else
            {
                buffer.Dispose();
            }
        }

        ops.OnSignalPullOutbound();
    }

    private void WriteToOutputPipe(TransportBuffer data)
    {
        var mem = _connection!.OutputWriter.GetMemory(data.Length);
        data.Memory.Span.CopyTo(mem.Span);
        _connection.OutputWriter.Advance(data.Length);
        data.Dispose();
        _ = _connection.OutputWriter.FlushAsync();
    }

    private void DisposeConnection()
    {
        if (_connection is not null)
        {
            _ = _connection.DisposeAsync().AsTask();
        }
    }
}
