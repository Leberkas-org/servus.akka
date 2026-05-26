using System.Net;

namespace Servus.Akka.Transport;

public interface ITransportInbound;

public sealed record TransportConnected(ConnectionInfo Info) : ITransportInbound;

public sealed record TransportDisconnected(DisconnectReason Reason) : ITransportInbound;

public sealed record TransportError(Exception Exception, bool Fatal) : ITransportInbound;

public sealed record StreamOpened(StreamTarget Id, StreamDirection Direction) : ITransportInbound;

public sealed record StreamClosed(StreamTarget Id, DisconnectReason Reason) : ITransportInbound;

public sealed record StreamReadCompleted(StreamTarget Id) : ITransportInbound;

public sealed record ServerStreamAccepted(StreamTarget Id, StreamDirection Direction) : ITransportInbound;

public sealed record ConnectionMigrationDetected(EndPoint OldEndPoint, EndPoint NewEndPoint) : ITransportInbound;
