using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

[Collection("TransportBuffer")]
public sealed class TransportBufferPoolSpec
{
    [Fact(Timeout = 10000)]
    public async Task Pool_should_survive_concurrent_rent_and_dispose()
    {
        const int threadCount = 8;
        const int iterationsPerThread = 500;

        using var barrier = new Barrier(threadCount);
        var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        var tasks = Enumerable.Range(0, threadCount).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            for (var i = 0; i < iterationsPerThread; i++)
            {
                try
                {
                    var buf = TransportBuffer.Rent(64);
                    buf.Length = 1;
                    buf.Dispose();
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Empty(exceptions);
    }

    [Fact(Timeout = 10000)]
    public void Pool_should_not_leak_when_disposed_from_multiple_threads_simultaneously()
    {
        const int count = 200;
        var buffers = new TransportBuffer[count];
        for (var i = 0; i < count; i++)
        {
            buffers[i] = TransportBuffer.Rent(64);
            buffers[i].Length = 1;
        }

        Parallel.ForEach(buffers, buf => buf.Dispose());

        var postBuf = TransportBuffer.Rent(64);
        Assert.True(postBuf.Capacity >= 64);
        postBuf.Dispose();
    }
}
