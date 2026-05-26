using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace Servus.Akka.TestKit;

public static class TestPipeline
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);

    public static async Task<TOut> RunAsync<TIn, TOut>(
        Flow<TIn, TOut, NotUsed> flow,
        TIn input,
        IMaterializer materializer,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        var result = Source.Single(input)
            .Via(flow)
            .RunWith(Sink.First<TOut>(), materializer);

        return await result.WaitAsync(timeout ?? DefaultTimeout, ct).ConfigureAwait(false);
    }

    public static async Task<IReadOnlyList<TOut>> RunManyAsync<TIn, TOut>(
        Flow<TIn, TOut, NotUsed> flow,
        IEnumerable<TIn> inputs,
        int expectedCount,
        IMaterializer materializer,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        var result = Source.From(inputs)
            .Via(flow)
            .Take(expectedCount)
            .RunWith(Sink.Seq<TOut>(), materializer);

        return await result.WaitAsync(timeout ?? DefaultTimeout, ct).ConfigureAwait(false);
    }
}
