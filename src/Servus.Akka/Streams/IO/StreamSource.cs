using Akka;
using Akka.Streams.Dsl;

namespace Servus.Akka.Streams.IO;

public static class StreamSource
{
    public static Source<ReadOnlyMemory<byte>, NotUsed> From(Stream stream, int bufferSize = 8 * 1024)
        => Source.FromGraph(new StreamSourceStage(stream, bufferSize));
}