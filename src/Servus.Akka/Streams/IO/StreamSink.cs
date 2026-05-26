using Akka.Streams.Dsl;

namespace Servus.Akka.Streams.IO;

public static class StreamSink
{
    public static Sink<ReadOnlyMemory<byte>, Task> To(Stream stream)
    {
        return Sink.FromGraph(new StreamSinkStage(stream));
    }
}