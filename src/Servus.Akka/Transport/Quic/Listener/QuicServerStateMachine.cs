using System.Buffers;
using System.Net;
using Akka.Actor;

namespace Servus.Akka.Transport.Quic.Listener;

internal sealed class QuicServerStateMachine(
    IConnectionOperations ops,
    IActorRef self,
    QuicConnectionHandle connectionHandle,
    ConnectionInfo connectionInfo)
{
    private const string MigrationCheckTimerKey = "migration-check";
    private static readonly TimeSpan MigrationCheckInterval = TimeSpan.FromSeconds(5);

    private int _connectionGen;
    private bool _upstreamFinished;
    private EndPoint? _lastRemoteEndPoint;
    private CancellationTokenSource? _acceptCts;

    private readonly Dictionary<StreamTarget, QuicStreamState> _streams = new();

    public void Start()
    {
        _connectionGen++;
        _lastRemoteEndPoint = connectionHandle.RemoteEndPoint();
        ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);

        StartAcceptLoop();

        ops.OnPushInbound(new TransportConnected(connectionInfo));
    }

    internal void Dispatch(IQuicTransportEvent evt)
    {
        switch (evt)
        {
            case PipeStreamReadComplete e:
                if (e.Gen == _connectionGen)
                {
                    OnPipeStreamReadComplete(e);
                }
                break;
            case PipeStreamReadFailed e:
                if (e.Gen == _connectionGen)
                {
                    OnPipeStreamReadFailed(e);
                }
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
        var state = new QuicStreamState(direction);
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
                _ = state.DisposeAsync();
            }
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

    private void OnStreamLeaseAcquired(Stream stream, long rawStreamId)
    {
        var streamId = StreamTarget.FromId(rawStreamId);
        if (!_streams.TryGetValue(streamId, out var state))
        {
            stream.Dispose();
            return;
        }

        state.AttachConnection(stream);
        if (state.Direction == StreamDirection.Bidirectional)
        {
            RequestStreamRead(streamId, _connectionGen);
        }

        ops.OnPushInbound(new StreamOpened(streamId, state.Direction));
    }

    private void OnInboundStreamAccepted(Stream stream, long rawStreamId)
    {
        var streamId = StreamTarget.FromId(rawStreamId);
        var direction = (rawStreamId & 0x02) != 0
            ? StreamDirection.Unidirectional
            : StreamDirection.Bidirectional;
        var state = new QuicStreamState(direction);
        state.AttachConnection(stream);
        _streams[streamId] = state;

        RequestStreamRead(streamId, _connectionGen);
        ops.OnPushInbound(new ServerStreamAccepted(streamId, direction));
    }

    private void RequestStreamRead(StreamTarget streamId, int gen)
    {
        if (!_streams.TryGetValue(streamId, out var state) || state.InputReader is null)
        {
            return;
        }

        state.AdvancePendingRead();

        state.InputReader.ReadAsync().AsTask().PipeTo(self,
            success: result => new PipeStreamReadComplete(result, streamId.Value, gen),
            failure: ex => new PipeStreamReadFailed(ex, streamId.Value, gen));
    }

    private void OnPipeStreamReadComplete(PipeStreamReadComplete evt)
    {
        var streamId = StreamTarget.FromId(evt.StreamId);
        if (!_streams.TryGetValue(streamId, out var state))
        {
            return;
        }

        var result = evt.Result;
        if (result.Buffer.Length > 0)
        {
            var length = (int)result.Buffer.Length;
            var buf = TransportBuffer.Rent(length);
            result.Buffer.CopyTo(buf.FullMemory.Span);
            buf.Length = length;
            state.SetPendingAdvance(result.Buffer.End);
            ops.OnPushInbound(new MultiplexedData(buf, streamId));
        }

        if (result.IsCompleted || result.IsCanceled)
        {
            OnInboundComplete(DisconnectReason.Graceful, evt.StreamId);
            return;
        }

        RequestStreamRead(streamId, _connectionGen);
    }

    private void OnPipeStreamReadFailed(PipeStreamReadFailed evt)
    {
        OnInboundComplete(DisconnectReason.Error, evt.StreamId);
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

    private void HandleConnectionFailure(DisconnectReason reason)
    {
        foreach (var (target, state) in _streams)
        {
            ops.OnPushInbound(new StreamClosed(target, reason));
            _ = state.DisposeAsync();
        }

        _streams.Clear();

        ops.OnPushInbound(new TransportDisconnected(reason));
        StopAcceptLoop();

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
    }

    private void Cleanup()
    {
        _connectionGen++;
        StopAcceptLoop();

        foreach (var (_, state) in _streams)
        {
            _ = state.DisposeAsync();
        }

        _streams.Clear();

        _ = connectionHandle.DisposeAsync();
    }
}
