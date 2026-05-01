using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Servus.Core.Diagnostics;

namespace Servus.Akka.Tests;

public class ContextExtensionTests : TestKit
{
    public class TestParentActor : ReceiveActor
    {
        public TestParentActor(IActorRef testActor)
        {
            Context.ResolveChildActor<TestChildActor>("hans");
            Receive<int>(msg => { Context.ChildForward("hans", msg); });

            Receive<float>(msg => { Context.ChildTell("hans", msg); });

            Receive<float>(msg => { Context.ChildTell("hans", msg); });

            Receive<bool>(msg =>
            {
                Assert.True(msg);
                testActor.Tell(true);
            });

            Receive<SimpleTracedMessage>(msg => { Context.ChildForwardTraced("hans", msg); }, m => m.Forward);

            Receive<SimpleTracedMessage>(msg => { Context.ChildTellTraced("hans", msg); }, m => !m.Forward);
        }
    }

    public class TestChildActor : ReceiveActor
    {
        public TestChildActor()
        {
            Receive<int>(msg =>
            {
                Assert.Equal(555, msg);
                Sender.Tell("hello");
            });

            Receive<float>(msg => { Sender.Tell(true); });

            Receive<SimpleTracedMessage>(msg =>
            {
                if (msg.Forward)
                {
                    Sender.Tell("hello");
                }
                else
                {
                    Sender.Tell(true);
                }
            });
        }
    }

    [Fact]
    public void ChildForwardTest()
    {
        var actor = Sys.ResolveActor<TestParentActor>(Nobody.Instance);
        actor.Tell(555);
        ExpectMsg("hello", cancellationToken: TestContext.Current.CancellationToken);
    }

    [Fact]
    public void ChildForwardTracedTest()
    {
        var actor = Sys.ResolveActor<TestParentActor>(Nobody.Instance);
        actor.Tell(new SimpleTracedMessage("hello", true));
        ExpectMsg("hello", cancellationToken: TestContext.Current.CancellationToken);
    }

    [Fact]
    public void ChildTellTest()
    {
        var a = CreateTestProbe("test");
        var actor = Sys.ResolveActor<TestParentActor>(a);
        actor.Tell(555f);

        a.ExpectMsg(true, cancellationToken: TestContext.Current.CancellationToken);
    }

    [Fact]
    public void ChildTellTracedTest()
    {
        var a = CreateTestProbe("test");
        var actor = Sys.ResolveActor<TestParentActor>(a);
        actor.Tell(new SimpleTracedMessage("hello"));
        a.ExpectMsg(true, cancellationToken: TestContext.Current.CancellationToken);
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithResolvableActor<TestParentActor>();
    }
}

public record SimpleTracedMessage(string Message, bool Forward = false) : IWithTracing
{
    public string? TraceId { get; set; }
    public string? SpanId { get; set; }
}