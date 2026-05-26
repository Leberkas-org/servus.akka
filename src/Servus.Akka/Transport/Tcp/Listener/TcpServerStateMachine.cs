using System.Buffers;
using System.Net.Security;
using Akka.Actor;

namespace Servus.Akka.Transport.Tcp.Listener;

internal sealed class TcpServerStateMachine
{
    private readonly ITransportOperations _ops;
    private readonly IActorRef _self;
    private readonly ClientState _state;
    private readonly ConnectionInfo _connectionInfo;
    private readonly SslStream? _sslStream;
    private readonly bool _allowDelayedNegotiation;

    private ConnectionHandle? _handle;
    private int _connectionGen;
    private bool _upstreamFinished;
    private TcpPumpManager? _pumpManager;

    public TcpServerStateMachine(
        ITransportOperations ops,
        IActorRef self,
        ClientState state,
        ConnectionInfo connectionInfo,
        SslStream? sslStream = null,
        bool allowDelayedNegotiation = false)
    {
        _ops = ops;
        _self = self;
        _state = state;
        _connectionInfo = connectionInfo;
        _sslStream = sslStream;
        _allowDelayedNegotiation = allowDelayedNegotiation;
    }

    public void Start()
    {
        _connectionGen++;
        _handle = new ConnectionHandle(_state.OutboundWriter, _state.InboundReader, CancellationToken.None);

        _pumpManager = new TcpPumpManager(_self);
        _pumpManager.StartPumps(_state, _connectionGen);

        if (_sslStream is not null || _allowDelayedNegotiation)
        {
            var baseSecurity = _connectionInfo.Security;
            var security = baseSecurity is not null
                ? baseSecurity with { SslStream = _sslStream, AllowDelayedNegotiation = _allowDelayedNegotiation }
                : new SecurityInfo(default, default, SslStream: _sslStream, AllowDelayedNegotiation: _allowDelayedNegotiation);
            _ops.OnPushInbound(new TransportConnected(_connectionInfo with { Security = security }));
        }
        else
        {
            _ops.OnPushInbound(new TransportConnected(_connectionInfo));
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
                _ops.OnCompleteStage();
                break;
            default:
                _ops.OnSignalPullOutbound();
                break;
        }
    }

    public void HandleUpstreamFinish()
    {
        _upstreamFinished = true;
        Cleanup();
        _ops.OnCompleteStage();
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
            _ops.OnSignalPullOutbound();
            return;
        }

        _handle.Write(data.Buffer);
        _ops.OnSignalPullOutbound();
    }

    private void OnInboundBatch(ITransportInbound[] batch, int count)
    {
        for (var i = 0; i < count; i++)
        {
            _ops.OnPushInbound(batch[i]);
            batch[i] = null!;
        }

        ArrayPool<ITransportInbound>.Shared.Return(batch);
    }

    private void OnInboundComplete(DisconnectReason reason)
    {
        _ops.OnPushInbound(new TransportDisconnected(reason));
        _pumpManager?.StopPumps();
        _handle = null;

        if (_upstreamFinished)
        {
            _ops.OnCompleteStage();
        }
        else
        {
            _ops.OnSignalPullOutbound();
        }
    }

    private void OnOutboundWriteFailed()
    {
        _ops.OnPushInbound(new TransportDisconnected(DisconnectReason.Error));
        _pumpManager?.StopPumps();
        _handle = null;
        _ops.OnSignalPullOutbound();
    }

    private void Cleanup()
    {
        _connectionGen++;
        _pumpManager?.StopPumps();
        _pumpManager = null;
        _handle = null;
        _state.Dispose();
    }
}
