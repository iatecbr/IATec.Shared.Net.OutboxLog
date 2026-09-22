using Xunit;

namespace IATec.Shared.Net.OutboxLog.IntegrationTests;

/// <summary>
/// Smoke test verifying the integration test project builds and references
/// the library. Real SQL Server (Testcontainers) tests are added in later tasks.
/// </summary>
public class ReferenceSmokeTests
{
    [Fact]
    public void IntegrationTestProjectBuilds()
    {
        var assembly = typeof(ReferenceSmokeTests).Assembly;
        Assert.NotNull(assembly);
    }
}
