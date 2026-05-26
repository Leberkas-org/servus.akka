namespace Servus.Akka.Transport.Tcp.Client;

internal sealed class AbruptCloseException() : Exception("Connection closed abruptly.");
