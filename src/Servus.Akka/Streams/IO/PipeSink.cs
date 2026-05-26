using System.IO.Pipelines;
using Akka.Streams.Dsl;

namespace Servus.Akka.Streams.IO;

public static class PipeSink
{
    public static Sink<ReadOnlyMemory<byte>, Task> To(PipeWriter writer)
    {
        return Sink.FromGraph(new PipeSinkStage(writer));
    }
}