using System.Buffers;
using System.Net.Security;
using Akka.Actor;

namespace Servus.Akka.Transport.Tcp.Listener;

internal sealed class TcpServerStateMachine(
    ITransportOperations ops,
    IActorRef self,
    ClientState state,
    ConnectionInfo connectionInfo,
    SslStream? sslStream = null,
    bool allowDelayedNegotiation = false)
{
    private ConnectionHandle? _handle;
    private int _connectionGen;
    private bool _upstreamFinished;
    private TcpPumpManager? _pumpManager;

    public void Start()
    {
        _connectionGen++;
        _handle = new ConnectionHandle(state.OutboundWriter, state.InboundReader, CancellationToken.None);

        _pumpManager = new TcpPumpManager(self);
        _pumpManager.StartPumps(state, _connectionGen);

        if (sslStream is not null || allowDelayedNegotiation)
        {
            var baseSecurity = connectionInfo.Security;
            var security = baseSecurity is not null
                ? baseSecurity with { SslStream = sslStream, AllowDelayedNegotiation = allowDelayedNegotiation }
                : new SecurityInfo(default, default, SslStream: sslStream, AllowDelayedNegotiation: allowDelayedNegotiation);
            ops.OnPushInbound(new TransportConnected(connectionInfo with { Security = security }));
        }
        else
        {
            ops.OnPushInbound(new TransportConnected(connectionInfo));
        }
    }

    internal void Dispatch(ITcpTransportEvent evt)
    {
        switch (evt)
        {
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
            case OutboundWriteFailed:
                OnOutboundWriteFailed();
                break;
        }
    }

    public void HandlePush(ITransportOutbound item)
    {
        switch (item)
        {
            case TransportData data:
                HandleTransportData(data);
                break;
            case DisconnectTransport:
                Cleanup();
                ops.OnCompleteStage();
                break;
            default:
                ops.OnSignalPullOutbound();
                break;
        }
    }

    public void HandleUpstreamFinish()
    {
        _upstreamFinished = true;
        Cleanup();
        ops.OnCompleteStage();
    }

    public void HandleDownstreamFinish()
    {
        Cleanup();
    }

    public void PostStop()
    {
        Cleanup();
    }

    private void HandleTransportData(TransportData data)
    {
        if (_handle is null)
        {
            data.Buffer.Dispose();
            ops.OnSignalPullOutbound();
            return;
        }

        _handle.Write(data.Buffer);
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
        ops.OnPushInbound(new TransportDisconnected(reason));
        _pumpManager?.StopPumps();
        _handle = null;

        if (_upstreamFinished)
        {
            ops.OnCompleteStage();
        }
        else
        {
            ops.OnSignalPullOutbound();
        }
    }

    private void OnOutboundWriteFailed()
    {
        ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Error));
        _pumpManager?.StopPumps();
        _handle = null;
        ops.OnSignalPullOutbound();
    }

    private void Cleanup()
    {
        _connectionGen++;
        _pumpManager?.StopPumps();
        _pumpManager = null;
        _handle = null;
        state.Dispose();
    }
}
