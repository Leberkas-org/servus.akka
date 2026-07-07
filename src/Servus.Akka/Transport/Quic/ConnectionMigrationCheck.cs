using System.Net;

namespace Servus.Akka.Transport.Quic;

/// <summary>
/// Connection-migration detection shared by the client and server QUIC state machines — both ran the
/// identical timer + remote-endpoint comparison verbatim. Pure function (mirrors <see cref="AdaptiveHint"/>):
/// callers own <paramref name="lastRemoteEndPoint"/> as their own field and pass it by reference so the
/// "last known" endpoint updates in place the moment a migration is detected.
/// </summary>
internal static class ConnectionMigrationCheck
{
    public static bool TryDetect(
        EndPoint? currentRemote,
        ref EndPoint? lastRemoteEndPoint,
        out EndPoint oldEndPoint,
        out EndPoint newEndPoint)
    {
        if (currentRemote is not null && lastRemoteEndPoint is not null && !currentRemote.Equals(lastRemoteEndPoint))
        {
            oldEndPoint = lastRemoteEndPoint;
            newEndPoint = currentRemote;
            lastRemoteEndPoint = currentRemote;
            return true;
        }

        oldEndPoint = null!;
        newEndPoint = null!;
        return false;
    }
}
