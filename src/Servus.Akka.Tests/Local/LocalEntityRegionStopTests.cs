using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Persistence;
using Servus.Akka.Local;

namespace Servus.Akka.Tests.Local;

/// <summary>A clock the test moves by hand.</summary>
public sealed class ManualTimeProvider : TimeProvider
{
    public DateTimeOffset Now { get; set; } = DateTimeOffset.UtcNow;
    public override DateTimeOffset GetUtcNow() => Now;
}

public sealed record Work(string EntityId, string Payload, ManualResetEventSlim? Gate = null) : IWithEntityId;

public sealed record Done(string Payload, Guid Incarnation);

/// <summary>Replies to every message; blocks on its gate first when one is given.</summary>
public class GatedEntityActor : ReceiveActor
{
    public GatedEntityActor()
    {
        var incarnation = Guid.NewGuid();
        Receive<Work>(w =>
        {
            w.Gate?.Wait(TimeSpan.FromSeconds(10));
            Sender.Tell(new Done(w.Payload, incarnation));
        });
    }
}

public class LocalEntityRegionIdleStopTests : TestKit
{
    private readonly ManualTimeProvider _clock = new();

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithLocalEntityRegion<GatedEntityActor>(
            typeName: "gated",
            entityPropsFactory: _ => Props.Create(() => new GatedEntityActor()),
            messageExtractor: new EntityIdExtractor(),
            options: new LocalEntityRegionOptions
            {
                PassivateIdleEntityAfter = TimeSpan.FromSeconds(1),
                TimeProvider = _clock,
            });
    }

    [Fact]
    public async Task Idle_passivation_lets_the_entity_finish_its_queued_messages_and_buffers_new_ones()
    {
        var region = await Sys.GetActorAsync<GatedEntityActor>();
        using var gate = new ManualResetEventSlim();

        region.Tell(new Work("ready", "ping"), TestActor);
        await ExpectMsgAsync<Done>(cancellationToken: TestContext.Current.CancellationToken);
        region.Tell(new Work("a", "busy", gate), TestActor);
        region.Tell(new Work("a", "queued"), TestActor);
        await Task.Delay(300, TestContext.Current.CancellationToken); // routed before the clock moves
        _clock.Now += TimeSpan.FromMinutes(1);
        await Task.Delay(TimeSpan.FromMilliseconds(2500), TestContext.Current.CancellationToken); // passivation tick ran
        region.Tell(new Work("a", "during"), TestActor);
        gate.Set();

        var busy = await ExpectMsgAsync<Done>(cancellationToken: TestContext.Current.CancellationToken);
        var queued = await ExpectMsgAsync<Done>(cancellationToken: TestContext.Current.CancellationToken);
        var during = await ExpectMsgAsync<Done>(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(["busy", "queued", "during"], new[] { busy.Payload, queued.Payload, during.Payload });
        Assert.Equal(busy.Incarnation, queued.Incarnation);
        Assert.NotEqual(busy.Incarnation, during.Incarnation);
    }
}

public sealed record Save(string EntityId) : IWithEntityId;

public sealed record Count(string EntityId) : IWithEntityId;

public sealed record Saved(int Total);

public sealed record Stop
{
    public static readonly Stop Instance = new();
}

/// <summary>A persistent entity that asks its region to passivate it right after it started persisting.</summary>
public class SavingEntityActor : ReceivePersistentActor
{
    private int _total;

    public override string PersistenceId { get; }

    public SavingEntityActor(string entityId)
    {
        PersistenceId = "saving-" + entityId;
        Recover<int>(n => _total += n);
        Command<Save>(_ =>
        {
            var sender = Sender;
            Persist(1, n =>
            {
                _total += n;
                sender.Tell(new Saved(_total));
            });
            Context.Parent.Tell(new Passivate(Stop.Instance));
        });
        Command<Count>(_ => Sender.Tell(new Saved(_total)));
        Command<Stop>(_ => Context.Stop(Self));
    }
}

public class LocalEntityRegionRequestedPassivationTests : TestKit
{
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithLocalEntityRegion<SavingEntityActor>(
            typeName: "saving",
            entityPropsFactory: id => Props.Create(() => new SavingEntityActor(id)));
    }

    [Fact]
    public async Task Entity_passivating_itself_during_persist_still_replies_and_recovers_on_the_next_message()
    {
        var region = await Sys.GetActorAsync<SavingEntityActor>();

        region.Tell(new Save("p1"), TestActor);
        region.Tell(new Count("p1"), TestActor);

        Assert.Equal(1, (await ExpectMsgAsync<Saved>(cancellationToken: TestContext.Current.CancellationToken)).Total);
        Assert.Equal(1, (await ExpectMsgAsync<Saved>(cancellationToken: TestContext.Current.CancellationToken)).Total);
    }

    [Fact]
    public async Task Passivated_entity_is_not_restarted_until_a_message_arrives()
    {
        var region = await Sys.GetActorAsync<SavingEntityActor>();
        region.Tell(new Save("p2"), TestActor);
        await ExpectMsgAsync<Saved>(cancellationToken: TestContext.Current.CancellationToken);

        await AwaitAssertAsync(async () =>
            await Assert.ThrowsAsync<ActorNotFoundException>(() =>
                Sys.ActorSelection(region.Path / "p2").ResolveOne(TimeSpan.FromMilliseconds(200))),
            cancellationToken: TestContext.Current.CancellationToken);
        await Task.Delay(500, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<ActorNotFoundException>(() =>
            Sys.ActorSelection(region.Path / "p2").ResolveOne(TimeSpan.FromMilliseconds(200)));
    }

    [Fact]
    public async Task Passivate_from_outside_the_region_is_ignored()
    {
        var region = await Sys.GetActorAsync<SavingEntityActor>();
        region.Tell(new Count("p3"), TestActor);
        await ExpectMsgAsync<Saved>(cancellationToken: TestContext.Current.CancellationToken);

        region.Tell(new Passivate(Stop.Instance), TestActor);
        region.Tell(new Count("p3"), TestActor);

        await ExpectMsgAsync<Saved>(cancellationToken: TestContext.Current.CancellationToken);
        var entity = await Sys.ActorSelection(region.Path / "p3").ResolveOne(TimeSpan.FromSeconds(1));
        Assert.NotNull(entity);
    }
}
