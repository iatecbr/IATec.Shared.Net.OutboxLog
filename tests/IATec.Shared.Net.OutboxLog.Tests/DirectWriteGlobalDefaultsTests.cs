using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Verifica que IOutboxStore.WriteAsync (escritura directa, sin pasar por el logger) rellena
/// containerKey y userId desde la configuracion global (LogsOutboxOptions) cuando el payload no los
/// trae. Se ejercita con el InMemoryOutboxStore.
/// </summary>
public class DirectWriteGlobalDefaultsTests
{
    private static IReadOnlyList<OutboxEntry> All(InMemoryOutboxStore store)
        => store.GetPendingAsync(int.MaxValue, int.MaxValue).GetAwaiter().GetResult();

    [Fact]
    public void DirectWrite_FillsContainerKeyAndUserId_FromGlobalOptions()
    {
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        var options = new LogsOutboxOptions
        {
            ContainerKey = "global-container",
            UserIdProvider = _ => "global-user",
        };

        var store = new InMemoryOutboxStore(timeProvider: null, options: options, serviceProvider: serviceProvider);

        // Payload SIN containerKey ni userId (como en una escritura directa por el consumidor).
        var result = store.WriteAsync(new LogPayload
        {
            Source = "s",
            Owner = "o",
            Action = "a",
            Content = "contenido",
        }).GetAwaiter().GetResult();

        Assert.Equal(WriteOutcome.Persisted, result.Outcome);

        var entry = Assert.Single(All(store));
        Assert.Equal("global-container", entry.Payload.ContainerKey);
        Assert.Equal("global-user", entry.Payload.UserId);
        // Los demas campos se preservan.
        Assert.Equal("s", entry.Payload.Source);
        Assert.Equal("o", entry.Payload.Owner);
        Assert.Equal("a", entry.Payload.Action);
        Assert.Equal("contenido", entry.Payload.Content);
    }

    [Fact]
    public void DirectWrite_KeepsPayloadValues_WhenAlreadyProvided()
    {
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        var options = new LogsOutboxOptions
        {
            ContainerKey = "global-container",
            UserIdProvider = _ => "global-user",
        };

        var store = new InMemoryOutboxStore(timeProvider: null, options: options, serviceProvider: serviceProvider);

        // Payload que YA trae containerKey y userId: la config global no debe sobrescribirlos.
        var result = store.WriteAsync(new LogPayload
        {
            ContainerKey = "explicit-container",
            UserId = "explicit-user",
            Content = "contenido",
        }).GetAwaiter().GetResult();

        Assert.Equal(WriteOutcome.Persisted, result.Outcome);

        var entry = Assert.Single(All(store));
        Assert.Equal("explicit-container", entry.Payload.ContainerKey);
        Assert.Equal("explicit-user", entry.Payload.UserId);
    }

    [Fact]
    public void DirectWrite_WithoutOptions_LeavesPayloadUnchanged()
    {
        // Sin options: comportamiento anterior, no se rellena nada.
        var store = new InMemoryOutboxStore();

        var result = store.WriteAsync(new LogPayload { Content = "c" }).GetAwaiter().GetResult();

        Assert.Equal(WriteOutcome.Persisted, result.Outcome);
        var entry = Assert.Single(All(store));
        Assert.Equal("", entry.Payload.ContainerKey);
        Assert.Equal("", entry.Payload.UserId);
    }
}