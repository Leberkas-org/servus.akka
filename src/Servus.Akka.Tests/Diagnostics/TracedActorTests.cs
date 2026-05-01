using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Servus.Akka.Diagnostics;
using Servus.Akka.Messaging;
using Servus.Core.Diagnostics;

namespace Servus.Akka.Tests.Diagnostics;

public class TracedPongActor : TracedMessageActor
{
    public TracedPongActor()
    {
        Receive<TracedMessage>(msg => { ReplyTraced(new TracedMessage("pong")); });
        Receive<int>(msg => { ReplyTraced(666); });
    }
}

public class ExtendedTracedActor : TracedMessageActor
{
    public ExtendedTracedActor()
    {
        ReceiveAsync<TracedMessage>(async _ =>
        {
            ReplyTraced(new TracedMessage("pooong"));
            await Task.Delay(1);
        });
        ReceiveAsync(typeof(int), m => 555.Equals(m), async (_) =>
        {
            ReplyTraced(666);
            await Task.Delay(1);
        });
        Receive<string>(_ =>
        {
            BecomeStacked(() =>
            {
                Receive<int>(_ => { ReplyTraced(69420); });
                Receive<string>(_ => UnbecomeStacked());
            });
        }, m => m == "stack");
        ReceiveAsync<string>(
            n => n == "become",
            _ =>
            {
                Become(() =>
                {
                    ReceiveAnyAsync(_ =>
                    {
                        Reply("leberkas");
                        return Task.CompletedTask;
                    });
                });

                return Task.CompletedTask;
            });
    }
}

public sealed record TracedMessage(string Message) : IWithTracing
{
    public string? TraceId { get; set; }
    public string? SpanId { get; set; }
}

public class TracedActorTests : TestKit
{
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithResolvableActor<TracedPongActor>();
    }

    [Fact]
    public void SimplePingPongTest()
    {
        var actor = Sys.ResolveActor<TracedPongActor>();
        actor.TellTraced(new TracedMessage("ping"));

        ExpectMsg<TracedMessage>(cancellationToken: TestContext.Current.CancellationToken);
        var msg = (TracedMessage) LastMessage;

        Assert.NotNull(msg.TraceId);
        Assert.NotNull(msg.SpanId);
    }

    [Fact]
    public async Task ExtendedPingPongTest()
    {
        var actor = Sys.ResolveActor<TracedPongActor>();
        actor.TellTraced(555);
        var msg1 = GetExpectedMsg<TracedMessageEnvelope>("555");

        var msg = await actor.AskTraced<TracedMessage>(new TracedMessage("ping"));

        Assert.NotNull(msg.TraceId);
        Assert.NotNull(msg.SpanId);

        Assert.NotNull(msg1.TraceId);
        Assert.NotNull(msg1.SpanId);
        Assert.Equal(666, msg1.Message);
    }

    [Fact]
    public void BecomeStackedTest()
    {
        var actor = Sys.ResolveActor<ExtendedTracedActor>();
        actor.TellTraced(new TracedMessage("ping"));
        var msg = GetExpectedMsg<TracedMessage>("ping");

        actor.TellTraced("stack");
        actor.TellTraced(555);
        var msg1 = GetExpectedMsg<TracedMessageEnvelope>("555 reply");
        actor.TellTraced("something");
        actor.TellTraced(555);
        var msg2 = GetExpectedMsg<TracedMessageEnvelope>("555 reply after become stacked");

        Assert.Equal("pooong", msg.Message);
        Assert.NotNull(msg.TraceId);
        Assert.NotNull(msg.SpanId);

        Assert.Equal(69420, msg1.Message);
        Assert.Equal(666, msg2.Message);
    }

    [Fact]
    public void BecomeTest()
    {
        var actor = Sys.ResolveActor<ExtendedTracedActor>();
        actor.TellTraced(new TracedMessage("ping"));
        var msg = GetExpectedMsg<TracedMessage>("ping");

        actor.Tell("become", ActorRefs.NoSender);
        actor.TellTraced(555);

        var msg2 = GetExpectedMsg<string>("555 reply after become stacked");
        actor.TellTraced("123");
        var msg3 = GetExpectedMsg<string>("123 reply");

        Assert.Equal("pooong", msg.Message);
        Assert.NotNull(msg.TraceId);
        Assert.NotNull(msg.SpanId);

        Assert.Equal("leberkas", msg2);
        Assert.Equal("leberkas", msg3);
    }

    private T GetExpectedMsg<T>(string hint)
    {
        ExpectMsg<T>(hint: hint);
        return (T) LastMessage;
    }
}