using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Pattern;

namespace Servus.Akka.Tests;

public class CircuitBreakerExtensionTests : TestKit
{
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
    }

    [Fact]
    public void CreateCircuitBreaker_FromActorSystem_UsesDefaults()
    {
        var breaker = Sys.CreateCircuitBreaker();

        Assert.Multiple(
            () => Assert.Equal(5, breaker.MaxFailures),
            () => Assert.Equal(TimeSpan.FromSeconds(10), breaker.CallTimeout),
            () => Assert.Equal(TimeSpan.FromSeconds(30), breaker.ResetTimeout),
            () => Assert.True(breaker.IsClosed));
    }

    [Fact]
    public void CreateCircuitBreaker_FromActorSystem_AcceptsCustomValues()
    {
        var breaker = Sys.CreateCircuitBreaker(
            maxFailures: 3,
            callTimeout: TimeSpan.FromSeconds(5),
            resetTimeout: TimeSpan.FromMinutes(1));

        Assert.Multiple(
            () => Assert.Equal(3, breaker.MaxFailures),
            () => Assert.Equal(TimeSpan.FromSeconds(5), breaker.CallTimeout),
            () => Assert.Equal(TimeSpan.FromMinutes(1), breaker.ResetTimeout));
    }

    [Fact]
    public void CreateCircuitBreaker_Configure_AppliesExponentialBackoff()
    {
        var breaker = Sys.CreateCircuitBreaker(
            configure: b => b
                .WithExponentialBackoff(TimeSpan.FromMinutes(5))
                .WithRandomFactor(0.2));

        Assert.Multiple(
            () => Assert.Equal(TimeSpan.FromMinutes(5), breaker.MaxResetTimeout),
            () => Assert.Equal(0.2, breaker.RandomFactor));
    }

    [Fact]
    public async Task CreateCircuitBreaker_FromActorContext_UsesSystemScheduler()
    {
        var probe = CreateTestProbe();
        var actor = Sys.ActorOf(Props.Create(() => new BreakerCreatorActor(probe)));

        actor.Tell("create");

        var response = await probe.ExpectMsgAsync<BreakerCreated>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Multiple(
            () => Assert.Equal(5, response.Breaker.MaxFailures),
            () => Assert.True(response.Breaker.IsClosed));
    }

    [Fact]
    public async Task CircuitBreaker_OpensAfterMaxFailures()
    {
        var breaker = Sys.CreateCircuitBreaker(maxFailures: 2, callTimeout: TimeSpan.FromSeconds(1));

        for (var i = 0; i < 2; i++)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                breaker.WithCircuitBreaker(_ => Task.FromException<int>(new InvalidOperationException())));
        }

        Assert.True(breaker.IsOpen);
    }

    [Fact]
    public async Task CircuitBreaker_FailsFastWhenOpen()
    {
        var breaker = Sys.CreateCircuitBreaker(maxFailures: 1, callTimeout: TimeSpan.FromSeconds(1));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            breaker.WithCircuitBreaker(_ => Task.FromException<int>(new InvalidOperationException())));

        Assert.True(breaker.IsOpen);
        await Assert.ThrowsAsync<OpenCircuitException>(() =>
            breaker.WithCircuitBreaker(_ => Task.FromResult(42)));
    }

    [Fact]
    public async Task CircuitBreaker_SuccessfulCallKeepsClosed()
    {
        var breaker = Sys.CreateCircuitBreaker(maxFailures: 3);

        var result = await breaker.WithCircuitBreaker(_ => Task.FromResult(42));

        Assert.Multiple(
            () => Assert.Equal(42, result),
            () => Assert.True(breaker.IsClosed),
            () => Assert.Equal(0, breaker.CurrentFailureCount));
    }

    [Fact]
    public async Task CircuitBreaker_TracksFailureCount()
    {
        var breaker = Sys.CreateCircuitBreaker(maxFailures: 5);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            breaker.WithCircuitBreaker(_ => Task.FromException<int>(new InvalidOperationException())));

        Assert.Multiple(
            () => Assert.True(breaker.IsClosed),
            () => Assert.Equal(1, breaker.CurrentFailureCount));
    }

    [Fact]
    public async Task CircuitBreaker_SuccessResetsFailureCount()
    {
        var breaker = Sys.CreateCircuitBreaker(maxFailures: 5);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            breaker.WithCircuitBreaker(_ => Task.FromException<int>(new InvalidOperationException())));
        Assert.Equal(1, breaker.CurrentFailureCount);

        await breaker.WithCircuitBreaker(_ => Task.FromResult(42));
        Assert.Equal(0, breaker.CurrentFailureCount);
    }

    [Fact]
    public async Task CircuitBreaker_OnOpenCallback_FiresWhenBreakerOpens()
    {
        var opened = false;
        var breaker = Sys.CreateCircuitBreaker(
            maxFailures: 1,
            configure: b => b.OnOpen(() => opened = true));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            breaker.WithCircuitBreaker(_ => Task.FromException<int>(new InvalidOperationException())));

        AwaitCondition(() => opened, TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        Assert.True(breaker.IsOpen);
    }

    [Fact]
    public async Task CircuitBreaker_PipeToPattern_DeliversResultToActor()
    {
        var breaker = Sys.CreateCircuitBreaker();
        var probe = CreateTestProbe();

        _ = breaker.WithCircuitBreaker(_ => Task.FromResult("hello")).PipeTo(probe);

        var msg = await probe.ExpectMsgAsync<string>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("hello", msg);
    }

    [Fact]
    public async Task CircuitBreaker_PipeToPattern_DeliversFailureWhenOpen()
    {
        var breaker = Sys.CreateCircuitBreaker(maxFailures: 1);
        var probe = CreateTestProbe();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            breaker.WithCircuitBreaker(_ => Task.FromException<string>(new InvalidOperationException())));

        _ = breaker.WithCircuitBreaker(_ => Task.FromResult("ignored")).PipeTo(probe);

        var msg = await probe.ExpectMsgAsync<Status.Failure>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.IsType<OpenCircuitException>(msg.Cause);
    }

    private sealed record BreakerCreated(CircuitBreaker Breaker);

    private sealed class BreakerCreatorActor : ReceiveActor
    {
        public BreakerCreatorActor(IActorRef probe)
        {
            Receive<string>(_ =>
            {
                var breaker = Context.CreateCircuitBreaker();
                probe.Tell(new BreakerCreated(breaker));
            });
        }
    }
}
