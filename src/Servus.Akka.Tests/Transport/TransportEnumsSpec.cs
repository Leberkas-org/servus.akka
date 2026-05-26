using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class TransportEnumsSpec
{
    [Fact(Timeout = 5000)]
    public void DisconnectReason_should_have_five_values()
    {
        var values = Enum.GetValues<DisconnectReason>();

        Assert.Equal(5, values.Length);
    }

    [Fact(Timeout = 5000)]
    public void DisconnectReason_should_contain_Graceful()
    {
        Assert.True(Enum.IsDefined(DisconnectReason.Graceful));
    }

    [Fact(Timeout = 5000)]
    public void DisconnectReason_should_contain_Timeout()
    {
        Assert.True(Enum.IsDefined(DisconnectReason.Timeout));
    }

    [Fact(Timeout = 5000)]
    public void DisconnectReason_should_contain_Error()
    {
        Assert.True(Enum.IsDefined(DisconnectReason.Error));
    }

    [Fact(Timeout = 5000)]
    public void DisconnectReason_should_contain_Evicted()
    {
        Assert.True(Enum.IsDefined(DisconnectReason.Evicted));
    }

    [Fact(Timeout = 5000)]
    public void PoolAction_should_have_two_values()
    {
        var values = Enum.GetValues<PoolAction>();

        Assert.Equal(2, values.Length);
    }

    [Fact(Timeout = 5000)]
    public void PoolAction_should_contain_Reuse()
    {
        Assert.True(Enum.IsDefined(PoolAction.Reuse));
    }

    [Fact(Timeout = 5000)]
    public void PoolAction_should_contain_Dispose()
    {
        Assert.True(Enum.IsDefined(PoolAction.Dispose));
    }
}
