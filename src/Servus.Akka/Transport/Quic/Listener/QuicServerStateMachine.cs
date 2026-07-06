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

    private bool _upstreamFinished;
    private EndPoint? _lastRemoteEndPoint;
    private CancellationTokenSource? _acceptCts;

    private readonly Dictionary<StreamTarget, QuicStreamState> _streams = new();

    internal int ActiveStreamCount => _streams.Count;

    public void Start()
    {
        _lastRemoteEndPoint = connectionHandle.RemoteEndPoint();
        ops.OnScheduleTimer(MigrationCheckTimerKey, MigrationCheckInterval);

        StartAcceptLoop();

        ops.OnPushInbound(new TransportConnected(connectionInfo));
    }

    internal void Dispatch(IQuicTransportEvent evt)
    {
        switch (evt)
        {
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
        var state = QuicStreamState.Rent(direction, pipeOptions);
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
            if (state.Write(data.Buffer))
            {
                _ = state.FlushWrites();
            }
        }
        else
        {
            data.Buffer.Dispose();
        }

        data.Return();
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
        var state = QuicStreamState.Rent(direction, pipeOptions);
        state.AttachConnection(stream, rawStreamId);
        _streams[streamId] = state;

        ops.OnPushInbound(new ServerStreamAccepted(streamId, direction));
        RequestStreamRead(streamId);
    }

    internal QuicStreamState RegisterTestStream(long streamId, StreamDirection direction)
    {
        var target = StreamTarget.FromId(streamId);
        var state = QuicStreamState.Rent(direction, pipeOptions);
        state.ActivateWithoutConnection();
        state.ActivateDirectReadForTest(streamId);
        _streams[target] = state;
        return state;
    }

    internal QuicStreamState? GetStreamForTest(long streamId)
        => _streams.GetValueOrDefault(StreamTarget.FromId(streamId));

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

        state.BeginPipeRead();
        reader.ReadAsync().PipeTo(self,
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
        var state = evt.State;
        if (!state.CompleteRead(out _))
        {
            return;
        }

        var streamId = StreamTarget.FromId(state.StreamId);
        var result = evt.Result;

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

        OnInboundComplete(DisconnectReason.Error, state.StreamId);
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

        foreach (var (_, state) in _streams)
        {
            _ = state.DisposeAndReturnAsync();
        }

        _streams.Clear();

        _ = connectionHandle.DisposeAsync();
    }
}