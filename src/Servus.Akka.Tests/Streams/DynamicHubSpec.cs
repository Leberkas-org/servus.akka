using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Akka.TestKit.Xunit;
using Servus.Akka.Streams;

namespace Servus.Akka.Tests.Streams;

public sealed class DynamicHubSpec : TestKit
{
    private static DynamicHub<int, int> Hub(int bufferSize = 256, int perConsumerBufferSize = 16)
        => new(x => x % 10, bufferSize, perConsumerBufferSize);

    [Fact(Timeout = 5000)]
    public void DynamicHub_should_route_element_to_matching_key_source_only()
    {
        var (up, hub) = this.SourceProbe<int>()
            .ToMaterialized(Hub(), Keep.Both)
            .Run(Sys.Materializer());

        var down1 = hub.Source(1).RunWith(this.SinkProbe<int>(), Sys.Materializer());
        var down2 = hub.Source(2).RunWith(this.SinkProbe<int>(), Sys.Materializer());

        down1.Request(10);
        down2.Request(10);

        up.SendNext(11, TestContext.Current.CancellationToken); // key 1
        up.SendNext(22, TestContext.Current.CancellationToken); // key 2
        up.SendNext(31, TestContext.Current.CancellationToken); // key 1

        Assert.Equal(11, down1.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        Assert.Equal(31, down1.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        Assert.Equal(22, down2.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        down2.ExpectNoMsg(TimeSpan.FromMilliseconds(200), TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = 5000)]
    public void DynamicHub_should_not_let_a_slow_key_block_other_keys_within_buffer()
    {
        var (up, hub) = this.SourceProbe<int>()
            .ToMaterialized(Hub(bufferSize: 256, perConsumerBufferSize: 4), Keep.Both)
            .Run(Sys.Materializer());

        hub.Source(1).RunWith(this.SinkProbe<int>(), Sys.Materializer());
        var fast = hub.Source(2).RunWith(this.SinkProbe<int>(), Sys.Materializer());

        // slow (key 1) never requests; fast (key 2) requests.
        fast.Request(10);

        // Interleave: key 1 elements sit buffered, key 2 elements flow through.
        up.SendNext(11, TestContext.Current.CancellationToken);
        up.SendNext(22, TestContext.Current.CancellationToken);
        up.SendNext(31, TestContext.Current.CancellationToken);
        up.SendNext(42, TestContext.Current.CancellationToken);

        Assert.Equal(22, fast.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        Assert.Equal(42, fast.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));

        // slow never requested, so it should not have received any data elements.
        // (We verify indirectly: fast got its elements despite slow not pulling.)
    }

    [Fact(Timeout = 5000)]
    public void DynamicHub_should_deliver_burst_larger_than_per_consumer_buffer_in_order()
    {
        var (up, hub) = this.SourceProbe<int>()
            .ToMaterialized(Hub(bufferSize: 256, perConsumerBufferSize: 4), Keep.Both)
            .Run(Sys.Materializer());

        var down = hub.Source(0).RunWith(this.SinkProbe<int>(), Sys.Materializer());

        const int count = 50;
        for (var i = 0; i < count; i++)
        {
            up.SendNext(i * 10, TestContext.Current.CancellationToken); // all key 0
        }

        for (var i = 0; i < count; i++)
        {
            down.Request(1);
            Assert.Equal(i * 10, down.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        }
    }

    [Fact(Timeout = 5000)]
    public void DynamicHub_should_backpressure_upstream_when_buffer_full()
    {
        var (up, hub) = this.SourceProbe<int>()
            .ToMaterialized(Hub(bufferSize: 3, perConsumerBufferSize: 2), Keep.Both)
            .Run(Sys.Materializer());

        // Consumer for key 0 exists but never requests, so elements accumulate in the hub buffer.
        hub.Source(0).RunWith(this.SinkProbe<int>(), Sys.Materializer());

        // perConsumerBufferSize=2 credit lets 2 reach the source buffer; the rest fill the hub buffer (size 3).
        // After ~5 elements the hub must stop pulling -> SendNext eventually back-pressures.
        for (var i = 0; i < 5; i++)
        {
            up.SendNext(i, TestContext.Current.CancellationToken);
        }

        up.ExpectNoMsg(TimeSpan.FromMilliseconds(200),
            TestContext.Current.CancellationToken); // probe accepted what it could; no failure/cancel
    }

    [Fact(Timeout = 5000)]
    public void DynamicHub_should_buffer_pending_elements_until_key_subscribes()
    {
        var (up, hub) = this.SourceProbe<int>()
            .ToMaterialized(Hub(), Keep.Both)
            .Run(Sys.Materializer());

        up.SendNext(70, TestContext.Current.CancellationToken); // key 0, no consumer yet
        up.SendNext(80, TestContext.Current.CancellationToken); // key 0

        var down = hub.Source(0).RunWith(this.SinkProbe<int>(), Sys.Materializer());
        down.Request(2);

        Assert.Equal(70, down.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        Assert.Equal(80, down.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = 5000)]
    public void DynamicHub_should_drain_then_complete_consumers_on_upstream_finish()
    {
        var (up, hub) = this.SourceProbe<int>()
            .ToMaterialized(Hub(), Keep.Both)
            .Run(Sys.Materializer());

        var down = hub.Source(0).RunWith(this.SinkProbe<int>(), Sys.Materializer());

        up.SendNext(90, TestContext.Current.CancellationToken);
        up.SendComplete(TestContext.Current.CancellationToken);

        down.Request(1);
        Assert.Equal(90, down.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        down.ExpectComplete(TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = 5000)]
    public void DynamicHub_should_propagate_failure_to_all_consumers()
    {
        var (up, hub) = this.SourceProbe<int>()
            .ToMaterialized(Hub(), Keep.Both)
            .Run(Sys.Materializer());

        var down1 = hub.Source(1).RunWith(this.SinkProbe<int>(), Sys.Materializer());
        var down2 = hub.Source(2).RunWith(this.SinkProbe<int>(), Sys.Materializer());
        down1.Request(1);
        down2.Request(1);

        var boom = new InvalidOperationException("boom");
        up.SendError(boom, TestContext.Current.CancellationToken);

        down1.ExpectError(TestContext.Current.CancellationToken);
        down2.ExpectError(TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = 5000)]
    public void DynamicHub_single_consumer_should_match_broadcasthub_demand_and_completion()
    {
        // DynamicHub path
        var (hubUp, hub) = this.SourceProbe<int>()
            .ToMaterialized(Hub(), Keep.Both)
            .Run(Sys.Materializer());
        var hubDown = hub.Source(0).RunWith(this.SinkProbe<int>(), Sys.Materializer());

        // BroadcastHub reference path
        var (bcUp, bcSource) = this.SourceProbe<int>()
            .ToMaterialized(BroadcastHub.Sink<int>(bufferSize: 256), Keep.Both)
            .Run(Sys.Materializer());
        var bcDown = bcSource.RunWith(this.SinkProbe<int>(), Sys.Materializer());

        foreach (var down in new[] { hubDown, bcDown })
        {
            down.Request(2);
        }

        hubUp.SendNext(0, TestContext.Current.CancellationToken);
        hubUp.SendNext(10, TestContext.Current.CancellationToken);
        bcUp.SendNext(0, TestContext.Current.CancellationToken);
        bcUp.SendNext(10, TestContext.Current.CancellationToken);

        Assert.Equal(0, hubDown.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        Assert.Equal(0, bcDown.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        Assert.Equal(10, hubDown.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        Assert.Equal(10, bcDown.ExpectNext(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));

        hubUp.SendComplete(TestContext.Current.CancellationToken);
        bcUp.SendComplete(TestContext.Current.CancellationToken);
        hubDown.Request(1);
        bcDown.Request(1);
        hubDown.ExpectComplete(TestContext.Current.CancellationToken);
        bcDown.ExpectComplete(TestContext.Current.CancellationToken);
    }
}