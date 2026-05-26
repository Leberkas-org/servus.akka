using System.Net;
using Akka.Actor;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Tests.Transport.Quic.Client;

public sealed class QuicConnectionMigrationSpec
{

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9000-9")]
    public void QuicOptions_should_default_AllowConnectionMigration_to_true()
    {
        var options = new QuicTransportOptions { Host = "example.com", Port = 443 };
        Assert.True(options.AllowConnectionMigration);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9000-9")]
    public void QuicOptions_should_accept_AllowConnectionMigration_false()
    {
        var options = new QuicTransportOptions { Host = "example.com", Port = 443, AllowConnectionMigration = false };
        Assert.False(options.AllowConnectionMigration);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9000-9")]
    public void Dispatch_MigrationDetected_should_push_ConnectionMigrationDetected()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var oldEp = new IPEndPoint(IPAddress.Parse("10.0.0.1"), 12345);
        var newEp = new IPEndPoint(IPAddress.Parse("10.0.0.2"), 12345);

        sm.Dispatch(new MigrationDetected(oldEp, newEp));

        var migrationEvent = Assert.Single(ops.PushedInbound);
        var detected = Assert.IsType<ConnectionMigrationDetected>(migrationEvent);
        Assert.Equal(oldEp, detected.OldEndPoint);
        Assert.Equal(newEp, detected.NewEndPoint);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9000-9")]
    public void CheckForConnectionMigration_should_detect_remote_endpoint_change_on_timer()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var initialEp = new IPEndPoint(IPAddress.Parse("10.0.0.1"), 12345);
        var changedEp = new IPEndPoint(IPAddress.Parse("10.0.0.2"), 54321);
        var currentRemoteEp = initialEp;

        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: async ct => { await Task.Delay(Timeout.Infinite, ct); return null; },
            getLocalEndPoint: () => new IPEndPoint(IPAddress.Loopback, 9999),
            getRemoteEndPoint: () => currentRemoteEp,
            dispose: () => ValueTask.CompletedTask);

        var lease = new QuicConnectionLease(handle, 100);

        sm.HandlePush(new ConnectTransport(new QuicTransportOptions { Host = "example.com", Port = 443 }));
        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        ops.PushedInbound.Clear();

        currentRemoteEp = changedEp;
        sm.OnTimer("migration-check");

        Assert.Contains(ops.PushedInbound, i => i is ConnectionMigrationDetected);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9000-9")]
    public void CheckForConnectionMigration_should_not_detect_when_remote_endpoint_unchanged()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var stableEp = new IPEndPoint(IPAddress.Parse("10.0.0.1"), 12345);

        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: async ct => { await Task.Delay(Timeout.Infinite, ct); return null; },
            getLocalEndPoint: () => new IPEndPoint(IPAddress.Loopback, 9999),
            getRemoteEndPoint: () => stableEp,
            dispose: () => ValueTask.CompletedTask);

        var lease = new QuicConnectionLease(handle, 100);

        sm.HandlePush(new ConnectTransport(new QuicTransportOptions { Host = "example.com", Port = 443 }));
        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        ops.PushedInbound.Clear();

        sm.OnTimer("migration-check");

        Assert.DoesNotContain(ops.PushedInbound, i => i is ConnectionMigrationDetected);
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9000-9")]
    public void InboundData_should_not_trigger_migration_check()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var changedEp = new IPEndPoint(IPAddress.Parse("10.0.0.2"), 54321);

        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: async ct => { await Task.Delay(Timeout.Infinite, ct); return null; },
            getLocalEndPoint: () => new IPEndPoint(IPAddress.Loopback, 9999),
            getRemoteEndPoint: () => changedEp,
            dispose: () => ValueTask.CompletedTask);

        var lease = new QuicConnectionLease(handle, 100);

        sm.HandlePush(new ConnectTransport(new QuicTransportOptions { Host = "example.com", Port = 443 }));
        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        ops.PushedInbound.Clear();

        var buf = TransportBuffer.Rent(4);
        buf.Length = 4;
        sm.Dispatch(new InboundData(buf, 0, 2));

        Assert.DoesNotContain(ops.PushedInbound, i => i is ConnectionMigrationDetected);

        var data = ops.PushedInbound.OfType<MultiplexedData>().FirstOrDefault();
        Assert.NotNull(data);
        data.Buffer.Dispose();
    }

    [Fact(Timeout = 5000)]
    [Trait("RFC", "RFC9000-9")]
    public void Timer_should_reschedule_after_migration_check()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var stableEp = new IPEndPoint(IPAddress.Parse("10.0.0.1"), 12345);

        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: async ct => { await Task.Delay(Timeout.Infinite, ct); return null; },
            getLocalEndPoint: () => new IPEndPoint(IPAddress.Loopback, 9999),
            getRemoteEndPoint: () => stableEp,
            dispose: () => ValueTask.CompletedTask);

        var lease = new QuicConnectionLease(handle, 100);

        sm.HandlePush(new ConnectTransport(new QuicTransportOptions { Host = "example.com", Port = 443 }));
        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        ops.Timers.Clear();
        sm.OnTimer("migration-check");

        Assert.True(ops.Timers.ContainsKey("migration-check"));
    }
}
