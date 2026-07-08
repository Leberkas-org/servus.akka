using System.Net;
using Akka.Actor;

namespace Servus.Akka.Transport.Quic.Listener;

internal sealed class QuicServerStateMachine : IQuicStreamReadHost
{
    private const string MigrationCheckTimerKey = "migration-check";
    private const int MaxSyncReads = 8;
    private static readonly TimeSpan MigrationCheckInterval = TimeSpan.FromSeconds(5);

    private readonly IConnectionOperations _ops;
    private readonly IActorRef _self;
    private readonly QuicConnectionHandle _connectionHandle;
    private readonly ConnectionInfo _connectionInfo;
    private readonly TransportConnectionOptions? _connectionOptions;
    private readonly QuicStreamLifecycle _lifecycle;

    private bool _upstreamFinished;
    private EndPoint? _lastRemoteEndPoint;
    private CancellationTokenSource? _acceptCts;

    public QuicServerStateMachine(
        IConnectionOperations ops,
        IActorRef self,
        QuicConnectionHandle connectionHandle,
        ConnectionInfo connectionInfo,
        TransportConnectionOptions? connectionOptions = null)
    {
        _ops = ops;
        _self = self;
        _connectionHandle = connectionHandle;
        _connectionInfo = connectionInfo;
        _connectionOptions = connectionOptions;
        _lifecycle = new QuicStreamLifecycle(ops, self, this, MaxSyncReads);
    }

    internal int ActiveStreamCount => _lifecycle.ActiveStreamCount;

    IConnectionOperations IQuicStreamReadHost.Ops => _ops;

    bool IQuicStreamReadHost.TryGetStream(StreamTarget id, out QuicStreamState state)
        => _lifecycle.TryGetStream(id, out state);

    void IQuicStreamReadHost.RequestStreamRead(StreamTarget streamId)
        => _lifecycle.RequestStreamRead(streamId);

    void IQuicStreamReadHost.OnInboundComplete(DisconnectReason reason, long rawStreamId)
        => _lifecycle.OnInboundComplete(reason, rawStreamId);

    void IQuicStreamReadHost.OnReadFailure(QuicStreamState state, Exception error)
        => _lifecycle.OnInboundComplete(DisconnectReason.Error, state.StreamId);

    public void Start()
    {
        _lastRemoteEndPoint = _connectionHandle.RemoteEndPoint();
        _ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);

        StartAcceptLoop();

        _ops.OnPushInbound(new TransportConnected(_connectionInfo));
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
                _lifecycle.OnInboundStreamAccepted(e.Stream, e.StreamId, _connectionOptions);
                break;
            case StreamLeaseAcquired e:
                _lifecycle.OnStreamLeaseAcquired(e.Stream, e.StreamId);
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
            case OpenStream open:
                _lifecycle.HandleOpenStream(open.StreamId, open.Direction, _connectionHandle, _connectionOptions);
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
                Cleanup();
                _ops.OnCompleteStage();
                break;
        }
    }

    public void HandleUpstreamFinish()
    {
        _upstreamFinished = true;
        StopAcceptLoop();
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

    internal void NotifyItemDelivered(StreamTarget streamId) => _lifecycle.NotifyItemDelivered(streamId);

    internal QuicStreamState RegisterTestStream(long streamId, StreamDirection direction)
        => _lifecycle.RegisterTestStream(streamId, direction, _connectionOptions);

    internal QuicStreamState? GetStreamForTest(long streamId) => _lifecycle.GetStream(streamId);

    private void CheckForConnectionMigration()
    {
        if (ConnectionMigrationCheck.TryDetect(
                _connectionHandle.RemoteEndPoint(), ref _lastRemoteEndPoint, out var old, out var current))
        {
            _ops.OnPushInbound(new ConnectionMigrationDetected(old, current));
        }
    }

    private void StartAcceptLoop()
    {
        _acceptCts?.Cancel();
        _acceptCts?.Dispose();
        _acceptCts = new CancellationTokenSource();
        _ = AcceptLoopAsync(_connectionHandle, _self, _acceptCts.Token);
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
                    return;
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
        _lifecycle.DisposeAllStreams();
        _ = _connectionHandle.DisposeAsync();
    }
}
