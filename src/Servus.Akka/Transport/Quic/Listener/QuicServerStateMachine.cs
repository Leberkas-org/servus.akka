using System.Buffers;
using System.Net;
using Akka.Actor;

namespace Servus.Akka.Transport.Quic.Listener;

internal sealed class QuicServerStateMachine(
    IConnectionOperations ops,
    IActorRef self,
    QuicConnectionHandle connectionHandle,
    ConnectionInfo connectionInfo,
    SocketPipeConnectionOptions? pipeOptions = null)
{
    private const string MigrationCheckTimerKey = "migration-check";
    private static readonly TimeSpan MigrationCheckInterval = TimeSpan.FromSeconds(5);

    private int _connectionGen;
    private bool _upstreamFinished;
    private EndPoint? _lastRemoteEndPoint;
    private CancellationTokenSource? _acceptCts;

    private readonly Dictionary<StreamTarget, QuicStreamState> _streams = new();

    internal int ActiveStreamCount => _streams.Count;

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
            case InboundStreamBatch e:
                foreach (var (stream, streamId) in e.Streams)
                {
                    OnInboundStreamAccepted(stream, streamId);
                }

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
        var state = new QuicStreamState(direction, pipeOptions);
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
        var state = new QuicStreamState(direction, pipeOptions);
        state.AttachConnection(stream);
        _streams[streamId] = state;

        RequestStreamRead(streamId, _connectionGen);
        ops.OnPushInbound(new ServerStreamAccepted(streamId, direction));
    }

    internal void RegisterTestStream(long streamId, StreamDirection direction)
    {
        var target = StreamTarget.FromId(streamId);
        var state = new QuicStreamState(direction, pipeOptions);
        state.ActivateWithoutConnection();
        _streams[target] = state;
    }

    private void RequestStreamRead(StreamTarget streamId, int gen)
    {
        if (!_streams.TryGetValue(streamId, out var state) || state.InputReader is null)
        {
            return;
        }

        var reader = state.InputReader;
        reader.ReadAsync().PipeTo(self,
            success: result =>
            {
                TransportBuffer? buf = null;
                if (result.Buffer.Length > 0)
                {
                    var length = (int)result.Buffer.Length;
                    buf = TransportBuffer.Rent(length);
                    result.Buffer.CopyTo(buf.FullMemory.Span);
                    buf.Length = length;
                }

                reader.AdvanceTo(result.Buffer.End);
                return new PipeStreamReadComplete(buf, streamId.Value, gen, result.IsCompleted || result.IsCanceled);
            },
            failure: ex => new PipeStreamReadFailed(ex, streamId.Value, gen));
    }

    private void OnPipeStreamReadComplete(PipeStreamReadComplete evt)
    {
        var streamId = StreamTarget.FromId(evt.StreamId);
        if (!_streams.TryGetValue(streamId, out _))
        {
            evt.Buffer?.Dispose();
            return;
        }

        if (evt.Buffer is not null)
        {
            ops.OnPushInbound(new MultiplexedData(evt.Buffer, streamId));
        }

        if (evt.IsCompleted)
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
                    continue;
                }

                var batch = new List<(Stream, long)>(4) { (result.Value.Stream, result.Value.StreamId) };
                await DrainPendingStreams(handle, batch, ct).ConfigureAwait(false);

                if (batch.Count == 1)
                {
                    self.Tell(new InboundStreamAccepted(batch[0].Item1, batch[0].Item2));
                }
                else
                {
                    self.Tell(new InboundStreamBatch(batch));
                }
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

    private static async Task DrainPendingStreams(
        QuicConnectionHandle handle, List<(Stream, long)> batch, CancellationToken ct)
    {
        const int maxBatch = 16;
        while (batch.Count < maxBatch)
        {
            try
            {
                using var probe = CancellationTokenSource.CreateLinkedTokenSource(ct);
                probe.CancelAfter(0);
                var next = await handle.AcceptInboundStreamAsync(probe.Token).ConfigureAwait(false);
                if (next is null)
                {
                    break;
                }

                batch.Add((next.Value.Stream, next.Value.StreamId));
            }
            catch (OperationCanceledException)
            {
                break;
            }
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