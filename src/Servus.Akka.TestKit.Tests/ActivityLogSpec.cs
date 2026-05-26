using System.Net;
using Servus.Akka.Transport;

namespace Servus.Akka.TestKit.Tests;

public sealed class ActivityLogSpec
{
    [Fact(Timeout = 5000)]
    public void Record_should_add_entry()
    {
        var log = new ActivityLog();
        var activity = new OutboundReceived(0, new TransportData(new byte[] { 0xAA }));

        log.Record(activity);

        Assert.Single(log.Entries);
        Assert.Same(activity, log.Entries[0]);
    }

    [Fact(Timeout = 5000)]
    public void OfType_should_filter_by_type()
    {
        var log = new ActivityLog();
        var outbound = new OutboundReceived(0, new TransportData(new byte[] { 0xAA }));
        var connectionInfo = new ConnectionInfo(
            new IPEndPoint(IPAddress.Loopback, 1000),
            new IPEndPoint(IPAddress.Loopback, 2000),
            TransportProtocol.Tcp);
        var inbound = new InboundPushed(0, new TransportConnected(connectionInfo));
        var handler = new HandlerInvoked("TestHandler", new TransportData(new byte[] { 0xBB }));

        log.Record(outbound);
        log.Record(inbound);
        log.Record(handler);
        log.Record(new StageCompleted());

        var outboundEntries = log.OfType<OutboundReceived>().ToList();
        Assert.Single(outboundEntries);
        Assert.Same(outbound, outboundEntries[0]);

        var inboundEntries = log.OfType<InboundPushed>().ToList();
        Assert.Single(inboundEntries);
        Assert.Same(inbound, inboundEntries[0]);

        var handlerEntries = log.OfType<HandlerInvoked>().ToList();
        Assert.Single(handlerEntries);
        Assert.Same(handler, handlerEntries[0]);
    }

    [Fact(Timeout = 5000)]
    public void Clear_should_remove_all_entries()
    {
        var log = new ActivityLog();
        log.Record(new OutboundReceived(0, new TransportData(new byte[] { 0xAA })));
        var connectionInfo = new ConnectionInfo(
            new IPEndPoint(IPAddress.Loopback, 1000),
            new IPEndPoint(IPAddress.Loopback, 2000),
            TransportProtocol.Tcp);
        log.Record(new InboundPushed(0, new TransportConnected(connectionInfo)));
        log.Record(new StageCompleted());

        Assert.Equal(3, log.Entries.Count);

        log.Clear();

        Assert.Empty(log.Entries);
    }

    [Fact(Timeout = 5000)]
    public void ListenerConnectionAccepted_should_set_properties()
    {
        var activity = new ListenerConnectionAccepted(42, true);

        Assert.Equal(42, activity.Index);
        Assert.True(activity.FromFactory);
        Assert.NotEqual(default, activity.Timestamp);
    }

    [Fact(Timeout = 5000)]
    public void Activity_Timestamp_should_be_utc()
    {
        var activity = new OutboundReceived(0, new TransportData(new byte[] { 0xAA }));

        Assert.Equal(DateTimeOffset.UtcNow.Offset, activity.Timestamp.Offset);
    }
}