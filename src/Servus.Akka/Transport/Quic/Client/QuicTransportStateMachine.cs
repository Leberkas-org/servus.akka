using System.Buffers;
using System.Net;
using Akka.Actor;
using static Servus.Senf;

namespace Servus.Akka.Transport.Quic.Client;

public sealed class QuicTransportStateMachine(
    IConnectionOperations ops,
    IActorRef connectionManager,
    IActorRef self)
{
    private const string ConnectTimerKey = "connect-timeout";
    private const string MigrationCheckTimerKey = "migration-check";
    private const int MaxSyncReads = 8;
    private static readonly TimeSpan MigrationCheckInterval = TimeSpan.FromSeconds(5);

    private QuicConnectionHandle? _connectionHandle;
    private QuicConnectionLease? _connectionLease;
    private ConnectTransport? _pendingConnect;
    private bool _autoReconnect;
    private SocketPipeConnectionOptions? _pipeOptions;
    private bool _upstreamFinished;
    private bool _isReconnecting;
    private CancellationTokenSource? _acquireCts;
    private CancellationTokenSource? _acceptCts;
    private EndPoint? _lastRemoteEndPoint;

    private readonly Dictionary<StreamTarget, QuicStreamState> _streams = new();
    private readonly HashSet<StreamTarget> _dirtyStreams = [];
    private int _syncReadBudget = MaxSyncReads;

    internal void Dispatch(IQuicTransportEvent evt)
    {
        switch (evt)
        {
            case ConnectionLeaseAcquired e:
                OnConnectionLeaseAcquired(e.Lease);
                break;
            case StreamLeaseAcquired e:
                OnStreamLeaseAcquired(e.Stream, e.StreamId);
                break;
            case AcquisitionFailed e:
                OnAcquisitionFailed(e.Error);
                break;
            case DirectStreamReadComplete e:
                OnDirectStreamReadComplete(e);
                break;
            case PipeStreamReadResult e:
                OnPipeStreamReadResult(e);
                break;
            case PipeStreamReadFailed e:
                OnPipeStreamReadFailed(e);
                break;
            case InboundStreamAccepted e:
                OnInboundStreamAccepted(e.Stream, e.StreamId);
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

        StopAcceptLoop();
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

        _pipeOptions = SocketPipeConnectionOptions.FromTransport(connect.Options);

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

        var state = QuicStreamState.Rent(direction, _pipeOptions);
        _streams[streamId] = state;

        var sid = streamId.Value;
        _connectionHandle.OpenStreamAsync(direction)
            .PipeTo(self,
                success: result => new StreamLeaseAcquired(result.Stream, sid),
                failure: ex => new AcquisitionFailed(ex));

        ops.OnSignalPullOutbound();
    }

    private void HandleMultiplexedData(MultiplexedData data)
    {
        if (_streams.TryGetValue(data.StreamId, out var state))
        {
            if (state.Write(data.Buffer))
            {
                _dirtyStreams.Add(data.StreamId);
            }
        }
        else
        {
            data.Buffer.Dispose();
        }

        data.Return();
        ops.OnSignalPullOutbound();
    }

    public void FlushBatch()
    {
        if (_dirtyStreams.Count == 0)
        {
            return;
        }

        foreach (var sid in _dirtyStreams)
        {
            if (_streams.TryGetValue(sid, out var s))
            {
                _ = s.FlushWrites();
            }
        }

        _dirtyStreams.Clear();
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

    private void OnConnectionLeaseAcquired(QuicConnectionLease lease)
    {
        ops.OnCancelTimer(ConnectTimerKey);
        _pendingConnect = null;
        _connectionLease = lease;
        _connectionHandle = lease.Handle;
        _lastRemoteEndPoint = _connectionHandle.RemoteEndPoint();
        ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);

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
        ops.OnPushInbound(new TransportConnected(info));
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
        var state = QuicStreamState.Rent(direction, _pipeOptions);
        state.AttachConnection(stream, rawStreamId);
        _streams[streamId] = state;

        ops.OnPushInbound(new ServerStreamAccepted(streamId, direction));
        RequestStreamRead(streamId);
    }

    internal QuicStreamState RegisterTestStream(long streamId, StreamDirection direction)
    {
        var target = StreamTarget.FromId(streamId);
        var state = QuicStreamState.Rent(direction, _pipeOptions);
        state.ActivateWithoutConnection();
        state.ActivateDirectReadForTest(streamId);
        _streams[target] = state;
        return state;
    }

    private void RequestStreamRead(StreamTarget streamId)
    {
        if (!_streams.TryGetValue(streamId, out var state))
        {
            return;
        }

        if (state.QuicStream is { } qs)
        {
            if (qs.ReadsClosed.IsCompleted)
            {
                OnInboundComplete(DisconnectReason.Graceful, streamId.Value);
                return;
            }

            var buf = TransportBuffer.Rent(state.ReadHint);
            state.BeginDirectRead(buf);
            qs.ReadAsync(buf.FullMemory, CancellationToken.None).PipeTo(self,
                success: state.DirectReadTransform,
                failure: state.FailureReadTransform);
            return;
        }

        if (state.InputReader is null)
        {
            return;
        }

        var reader = state.InputReader;

        if (state.PendingAdvance is { } pos)
        {
            state.PendingAdvance = null;
            reader.AdvanceTo(pos);
        }

        var readTask = reader.ReadAsync();

        if (readTask.IsCompletedSuccessfully && _syncReadBudget > 0)
        {
            _syncReadBudget--;
            ProcessPipeReadResult(state, readTask.Result);
            return;
        }

        _syncReadBudget = MaxSyncReads;
        state.BeginPipeRead();
        readTask.PipeTo(self,
            success: state.PipeReadTransform,
            failure: state.FailureReadTransform);
    }

    private void OnDirectStreamReadComplete(DirectStreamReadComplete evt)
    {
        var state = evt.State;
        if (!state.CompleteRead(out var buf))
        {
            // Stream torn down while the read was in flight; the pending buffer is released.
            return;
        }

        if (evt.BytesRead == 0 || buf is null)
        {
            buf?.Dispose();
            OnInboundComplete(DisconnectReason.Graceful, state.StreamId);
            return;
        }

        buf.Length = evt.BytesRead;
        state.AdaptReadHint(evt.BytesRead);
        var streamId = StreamTarget.FromId(state.StreamId);
        ops.OnPushInbound(MultiplexedData.Rent(buf, streamId));
        RequestStreamRead(streamId);
    }

    private void OnPipeStreamReadResult(PipeStreamReadResult evt)
    {
        if (!evt.State.CompleteRead(out _))
        {
            return;
        }

        ProcessPipeReadResult(evt.State, evt.Result);
    }

    private void ProcessPipeReadResult(QuicStreamState state, System.IO.Pipelines.ReadResult result)
    {
        var streamId = StreamTarget.FromId(state.StreamId);

        if (result.Buffer.Length > 0)
        {
            var length = (int)result.Buffer.Length;
            var buf = TransportBuffer.Rent(length);
            result.Buffer.CopyTo(buf.FullMemory.Span);
            buf.Length = length;
            ops.OnPushInbound(MultiplexedData.Rent(buf, streamId));
        }

        // Deferred like the TCP read pump: AdvanceTo runs on the actor right before the next
        // ReadAsync, so the pipe's internal buffers are never touched from an IO thread.
        state.PendingAdvance = result.Buffer.End;

        if (result.IsCompleted || result.IsCanceled)
        {
            OnInboundComplete(DisconnectReason.Graceful, state.StreamId);
            return;
        }

        RequestStreamRead(streamId);
    }

    private void OnPipeStreamReadFailed(PipeStreamReadFailed evt)
    {
        var state = evt.State;
        if (!state.CompleteRead(out _))
        {
            return;
        }

        // QuicException on read means the peer closed or reset the stream (FIN/STOP_SENDING/
        // RST_STREAM) — a graceful stream completion, not an error to propagate.
        if (evt.Error is System.Net.Quic.QuicException)
        {
            OnInboundComplete(DisconnectReason.Graceful, state.StreamId);
            return;
        }

        if (IsConnectionLevelError(evt.Error))
        {
            HandleConnectionFailure(DisconnectReason.Error);
        }
        else
        {
            OnInboundComplete(DisconnectReason.Error, state.StreamId);
        }
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
                _ = state.DisposeAndReturnAsync();
            }

            _streams.Clear();

            ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Transient));
            _isReconnecting = true;
            StopAcceptLoop();
            ReturnConnectionToPool(false);
            _connectionHandle = null;
            _connectionLease = null;
            ops.OnSignalPullOutbound();
            return;
        }

        foreach (var (target, state) in _streams)
        {
            ops.OnPushInbound(new StreamClosed(target, reason));
            _ = state.DisposeAndReturnAsync();
        }

        _streams.Clear();

        ops.OnPushInbound(new TransportDisconnected(reason));
        StopAcceptLoop();
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

    private void StartAcceptLoop(QuicConnectionHandle connectionHandle)
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
                // A null result here (the cancellation case is handled above) means
                // AcceptInboundStreamAsync threw — for QUIC that only happens when the connection is
                // aborted/idle/closed, which is terminal. Stop the loop instead of busy-spinning
                // (re-throwing a QuicException every iteration with no backoff); the connection failure
                // is surfaced by the stream read pumps and the pool's liveness check.
                return;
            }

            self.Tell(new InboundStreamAccepted(result.Value.Stream, result.Value.StreamId));
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

        connectionManager.Tell(new QuicConnectionManagerActor.Release(lease, canReuse));

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

        foreach (var (_, state) in _streams)
        {
            _ = state.DisposeAndReturnAsync();
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