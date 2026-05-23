using System.Threading.Channels;
using Akka.Actor;
using Akka.Event;
using Servus.Akka.Local;

namespace Servus.Akka.Samples.WebApi.Actors;

public class LogForwarderActor : ReceiveActor
{
    public LogForwarderActor(ChannelWriter<string> writer)
    {
        Receive<Info>(msg =>
        {
            if (msg.LogClass == typeof(LocalEntityRegionActor))
                writer.TryWrite($"[{msg.LogLevel()}] {msg.Message}");
        });
    }
}