using Akka.Util;

namespace Servus.Akka.Tests;

public class AkkaOptionsExtensions
{
    [Fact]
    public void SimpleMatchTest()
    {
        var a = Option<string>.Create("test");
        a.Match(b => Assert.Equal("test", b), () => Assert.Fail());

        var result = a.Match(_ => true, () => false);
        Assert.True(result);
    }

    [Fact]
    public void SimpleMatchTest_None()
    {
        var a = Option<string>.None;
        a.Match(_ => Assert.Fail(), () => Assert.True(true));

        var result = a.Match(_ => true, () => false);
        Assert.False(result);
    }
}