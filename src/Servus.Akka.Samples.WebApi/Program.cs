using System.Threading.Channels;
using Akka.Actor;
using Akka.Event;
using Akka.Hosting;
using Servus.Akka;
using Servus.Akka.Local;
using Servus.Akka.Samples.WebApi.Actors;

var logChannel = Channel.CreateBounded<string>(100);
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAkka("counter-system", akka =>
{
    akka.WithLocalEntityRegion<CounterActor>(
        typeName: "counters",
        entityPropsFactory: id => Props.Create(() => new CounterActor(id)),
        messageExtractor: new EntityIdExtractor(),
        options: new LocalEntityRegionOptions
        {
            PassivateIdleEntityAfter = TimeSpan.FromSeconds(20),
            EntityIdStore = new FileEntityIdStore(Environment.SpecialFolder.CommonApplicationData, "servus.akka.samples.webapi")
        });

    akka.WithActors((system, _) =>
    {
        var forwarder = system.ActorOf(Props.Create(() => new LogForwarderActor(logChannel.Writer)));
        system.EventStream.Subscribe(forwarder, typeof(Info));
    });
});

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/counters/{id}", async (string id, ActorSystem system) =>
{
    if (!LocalEntityRegionActor.IsValidEntityId(id))
        return Results.BadRequest(new { error = $"Invalid entity ID '{id}'" });

    var region = await system.GetActorAsync<CounterActor>();
    var result = await region.Ask<CounterValue>(new GetCount(id), TimeSpan.FromSeconds(5));
    return Results.Ok(result);
});

app.MapPost("/counters/{id}/increment", async (string id, ActorSystem system) =>
{
    if (!LocalEntityRegionActor.IsValidEntityId(id))
        return Results.BadRequest(new { error = $"Invalid entity ID '{id}'" });

    var region = await system.GetActorAsync<CounterActor>();
    var result = await region.Ask<CounterValue>(new Increment(id), TimeSpan.FromSeconds(5));
    return Results.Ok(result);
});

app.MapPost("/counters/{id}/decrement", async (string id, ActorSystem system) =>
{
    if (!LocalEntityRegionActor.IsValidEntityId(id))
        return Results.BadRequest(new { error = $"Invalid entity ID '{id}'" });

    var region = await system.GetActorAsync<CounterActor>();
    var result = await region.Ask<CounterValue>(new Decrement(id), TimeSpan.FromSeconds(5));
    return Results.Ok(result);
});

app.MapGet("/logs", async (HttpContext ctx, CancellationToken ct) =>
{
    ctx.Response.ContentType = "text/event-stream";
    var reader = logChannel.Reader;

    await foreach (var line in reader.ReadAllAsync(ct))
    {
        await ctx.Response.WriteAsync($"data: {line}\n\n", ct);
        await ctx.Response.Body.FlushAsync(ct);
    }
});

await app.RunAsync();
