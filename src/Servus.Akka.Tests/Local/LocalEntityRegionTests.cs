using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Servus.Akka.Local;

namespace Servus.Akka.Tests.Local;

public sealed record EntityMessage(string EntityId, string Payload) : IWithEntityId;

public class EchoEntityActor : ReceiveActor
{
    public EchoEntityActor(string entityId)
    {
        Receive<EntityMessage>(msg => Sender.Tell(new EntityResponse(entityId, msg.Payload)));
    }
}

public sealed record EntityResponse(string EntityId, string Payload);

public class LocalEntityRegionTests : TestKit
{
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithLocalEntityRegion<EchoEntityActor>(
            typeName: "echo",
            entityPropsFactory: id => Props.Create(() => new EchoEntityActor(id)));
    }

    [Fact]
    public void RoutesMessageToCorrectEntity()
    {
        var region = Sys.GetActor<EchoEntityActor>();

        region.Tell(new EntityMessage("order-1", "hello"), TestActor);
        var response = ExpectMsg<EntityResponse>(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("order-1", response.EntityId);
        Assert.Equal("hello", response.Payload);
    }

    [Fact]
    public void CreatesEntityOnFirstMessage()
    {
        var region = Sys.GetActor<EchoEntityActor>();

        region.Tell(new EntityMessage("order-new", "first"), TestActor);
        var response = ExpectMsg<EntityResponse>(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("order-new", response.EntityId);
        Assert.Equal("first", response.Payload);
    }

    [Fact]
    public void ReusesExistingEntity()
    {
        var region = Sys.GetActor<EchoEntityActor>();

        region.Tell(new EntityMessage("order-1", "first"), TestActor);
        var response1 = ExpectMsg<EntityResponse>(cancellationToken: TestContext.Current.CancellationToken);

        region.Tell(new EntityMessage("order-1", "second"), TestActor);
        var response2 = ExpectMsg<EntityResponse>(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("order-1", response1.EntityId);
        Assert.Equal("order-1", response2.EntityId);
        Assert.Equal("first", response1.Payload);
        Assert.Equal("second", response2.Payload);
    }

    [Fact]
    public void RoutesToDifferentEntities()
    {
        var region = Sys.GetActor<EchoEntityActor>();

        region.Tell(new EntityMessage("order-1", "hello"), TestActor);
        var r1 = ExpectMsg<EntityResponse>(cancellationToken: TestContext.Current.CancellationToken);

        region.Tell(new EntityMessage("order-2", "world"), TestActor);
        var r2 = ExpectMsg<EntityResponse>(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("order-1", r1.EntityId);
        Assert.Equal("order-2", r2.EntityId);
    }
}

public class LocalEntityRegionPassivationTests : TestKit
{
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithLocalEntityRegion<EchoEntityActor>(
            typeName: "echo-passivate",
            entityPropsFactory: id => Props.Create(() => new EchoEntityActor(id)),
            messageExtractor: new EntityIdExtractor(),
            options: new LocalEntityRegionOptions
            {
                PassivateIdleEntityAfter = TimeSpan.FromSeconds(1)
            });
    }

    [Fact]
    public async Task PassivatesIdleEntities()
    {
        var region = await Sys.GetActorAsync<EchoEntityActor>();

        region.Tell(new EntityMessage("order-1", "hello"), TestActor);
        await ExpectMsgAsync<EntityResponse>(cancellationToken: TestContext.Current.CancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);

        region.Tell(new EntityMessage("order-1", "after-passivation"), TestActor);
        var response = await ExpectMsgAsync<EntityResponse>(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("order-1", response.EntityId);
        Assert.Equal("after-passivation", response.Payload);
    }
}

public class LocalEntityRegionRecoveryTests : TestKit
{
    private static readonly InMemoryEntityIdStore Store = new();

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        Store.EntityStarted("pre-existing-1").GetAwaiter().GetResult();
        Store.EntityStarted("pre-existing-2").GetAwaiter().GetResult();

        builder.WithLocalEntityRegion<EchoEntityActor>(
            typeName: "echo-recover",
            entityPropsFactory: id => Props.Create(() => new EchoEntityActor(id)),
            messageExtractor: new EntityIdExtractor(),
            options: new LocalEntityRegionOptions
            {
                EntityIdStore = Store
            });
    }

    [Fact]
    public void RecoveredEntitiesRespondToMessages()
    {
        var region = Sys.GetActor<EchoEntityActor>();

        region.Tell(new EntityMessage("pre-existing-1", "hello"), TestActor);
        var response = ExpectMsg<EntityResponse>(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("pre-existing-1", response.EntityId);
        Assert.Equal("hello", response.Payload);
    }
}

public class LocalEntityRegionInvalidEntityIdTests : TestKit
{
    private class InvalidIdExtractor : IEntityIdExtractor
    {
        public string? EntityId(object message) => message as string;
        public object EntityMessage(object message) => message;
    }

    private class SinkActor : ReceiveActor
    {
        public SinkActor()
        {
            ReceiveAny(_ => Sender.Tell("ok"));
        }
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithLocalEntityRegion<SinkActor>(
            typeName: "invalid-id",
            entityPropsFactory: id => Props.Create(() => new SinkActor()),
            messageExtractor: new InvalidIdExtractor());
    }

    [Theory]
    [InlineData("order/1")]
    [InlineData("order#1")]
    [InlineData("order$1")]
    [InlineData("")]
    [InlineData("  ")]
    public void RejectsInvalidEntityIds(string invalidId)
    {
        var region = Sys.GetActor<SinkActor>();
        region.Tell(invalidId, TestActor);
        ExpectNoMsg(TimeSpan.FromMilliseconds(500), TestContext.Current.CancellationToken);
    }

    [Fact]
    public void AcceptsValidEntityIds()
    {
        var region = Sys.GetActor<SinkActor>();
        region.Tell("order-1", TestActor);
        ExpectMsg<string>("ok", cancellationToken: TestContext.Current.CancellationToken);
    }
}

public class LocalEntityRegionPassivationRaceTests : TestKit
{
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithLocalEntityRegion<EchoEntityActor>(
            typeName: "echo-race",
            entityPropsFactory: id => Props.Create(() => new EchoEntityActor(id)),
            messageExtractor: new EntityIdExtractor(),
            options: new LocalEntityRegionOptions
            {
                PassivateIdleEntityAfter = TimeSpan.FromSeconds(1)
            });
    }

    [Fact]
    public async Task MessageDuringPassivationIsNotLost()
    {
        var region = await Sys.GetActorAsync<EchoEntityActor>();

        region.Tell(new EntityMessage("order-1", "first"), TestActor);
        await ExpectMsgAsync<EntityResponse>(cancellationToken: TestContext.Current.CancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        // Entity is passivating or just passivated — send message immediately
        region.Tell(new EntityMessage("order-1", "after-race"), TestActor);
        var response = await ExpectMsgAsync<EntityResponse>(TimeSpan.FromSeconds(5), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("order-1", response.EntityId);
        Assert.Equal("after-race", response.Payload);
    }
}

public class LocalEntityRegionCustomExtractorTests : TestKit
{
    private class CustomExtractor : IEntityIdExtractor
    {
        public string? EntityId(object message) => message is string s ? s.Split(':')[0] : null;
        public object EntityMessage(object message) => message is string s ? s.Split(':')[1] : message;
    }

    public class StringEchoActor : ReceiveActor
    {
        public StringEchoActor(string entityId)
        {
            ReceiveAny(msg => Sender.Tell($"{entityId}:{msg}"));
        }
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithLocalEntityRegion<StringEchoActor>(
            typeName: "custom-extract",
            entityPropsFactory: id => Props.Create(() => new StringEchoActor(id)),
            messageExtractor: new CustomExtractor());
    }

    [Fact]
    public void UsesCustomExtractor()
    {
        var region = Sys.GetActor<StringEchoActor>();

        region.Tell("order-1:hello", TestActor);
        var response = ExpectMsg<string>(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("order-1:hello", response);
    }
}
