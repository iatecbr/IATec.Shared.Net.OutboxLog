using CsCheck;
using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.Net.OutboxLog.Dispatch;
using IATec.Shared.Net.OutboxLog.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Unit and property tests for <see cref="LogsOutboxServiceCollectionExtensions.AddLogsOutbox(IServiceCollection, System.Action{LogsOutboxOptions})"/>.
/// Covers service resolution for a valid in-memory configuration, applied defaults, rejection of
/// unsupported store types and SQL-without-DbContext, and completes the container-unmodified half of
/// Property 18 (registration count unchanged after a rejected configuration).
/// </summary>
public class LogsOutboxRegistrationTests
{
    // Req 1.2: con EnableLoggerProvider habilitado (opt-in), una configuracion valida resuelve
    // ILoggerProvider, IOutboxStore y el hosted DispatchWorker.
    [Fact]
    public void ValidInMemoryConfig_ResolvesLoggerProviderStoreAndDispatchWorker()
    {
        var services = new ServiceCollection();

        services.AddLogsOutbox(options =>
        {
            options.StoreType = OutboxStoreType.InMemory;
            options.EnableLoggerProvider = true; // opt-in: por defecto es false
        });

        using var provider = services.BuildServiceProvider();

        var loggerProvider = provider.GetRequiredService<ILoggerProvider>();
        Assert.IsType<OutboxLoggerProvider>(loggerProvider);

        var store = provider.GetRequiredService<IOutboxStore>();
        Assert.IsType<InMemoryOutboxStore>(store);

        var hostedServices = provider.GetServices<IHostedService>();
        Assert.Contains(hostedServices, hosted => hosted is DispatchWorker);
    }

    // La integracion con ILogger es opcional: con EnableLoggerProvider=false NO se registra el
    // OutboxLoggerProvider, pero el store y el worker de despacho siguen disponibles para envio
    // explicito via IOutboxStore / ILogBankClient.
    [Fact]
    public void LoggerProviderDisabled_DoesNotRegisterProvider_ButKeepsStoreAndWorker()
    {
        var services = new ServiceCollection();

        services.AddLogsOutbox(options =>
        {
            options.StoreType = OutboxStoreType.InMemory;
            options.EnableLoggerProvider = false;
        });

        using var provider = services.BuildServiceProvider();

        // No debe haber ningun ILoggerProvider de la libreria registrado.
        var loggerProviders = provider.GetServices<ILoggerProvider>();
        Assert.DoesNotContain(loggerProviders, p => p is OutboxLoggerProvider);

        // El store y el worker siguen registrados para uso directo.
        Assert.IsType<InMemoryOutboxStore>(provider.GetRequiredService<IOutboxStore>());
        Assert.Contains(provider.GetServices<IHostedService>(), hosted => hosted is DispatchWorker);
    }

    // Req 1.4, 1.5: defaults are applied when unspecified — RetryLimit == 3 and the default endpoint URL.
    [Fact]
    public void UnspecifiedOptions_ApplyDefaults_RetryLimitAndEndpoint()
    {
        var services = new ServiceCollection();

        // Configure callback sets nothing; every field must fall back to its default.
        services.AddLogsOutbox(_ => { });

        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<LogsOutboxOptions>();

        Assert.Equal(3, options.RetryLimit);
        Assert.Equal("https://api-is-logs-dev.sdasystems.org/v1/log", options.LogBankEndpoint);
    }

    // Req 1.7: an unsupported (undefined) store-type enum value is rejected identifying StoreType.
    [Fact]
    public void UnsupportedStoreType_IsRejected_IdentifyingStoreTypeField()
    {
        var services = new ServiceCollection();

        var ex = Assert.Throws<LogsOutboxConfigurationException>(
            () => services.AddLogsOutbox(options => options.StoreType = (OutboxStoreType)999));

        Assert.Equal(nameof(LogsOutboxOptions.StoreType), ex.FieldName);
    }

    // Req 3.8: the non-generic overload cannot bind the SQL store to a consumer DbContext, so
    // selecting the SQL store is rejected identifying the DbContext field.
    [Fact]
    public void SqlStoreWithoutDbContext_IsRejected_IdentifyingDbContextField()
    {
        var services = new ServiceCollection();

        var ex = Assert.Throws<LogsOutboxConfigurationException>(
            () => services.AddLogsOutbox(options => options.StoreType = OutboxStoreType.Sql));

        Assert.Equal("DbContext", ex.FieldName);
    }

    // Feature: logs-outbox-library, Property 18: Invalid configuration is rejected and leaves the container unmodified
    // Validates: Requirements 1.6, 1.8
    //
    // Completes the "container-unmodified" half of Property 18: for any rejected configuration
    // (out-of-range RetryLimit or a malformed endpoint), AddLogsOutbox throws and the service
    // collection's registration count is unchanged — no partial registration leaks into the container.
    [Fact]
    public void RejectedConfig_ThrowsAndLeavesContainerUnmodified()
    {
        // Two independent families of invalid input: RetryLimit outside [1, 10], and endpoints that
        // are not well-formed absolute http/https URLs. Each sample mutates exactly one field so the
        // rejection reason is unambiguous.
        Gen<int> outOfRangeRetry = Gen.OneOf(
            Gen.Int[int.MinValue, 0],
            Gen.Int[11, int.MaxValue]);

        Gen<string> malformedEndpoint = Gen.OneOf(
            Gen.Const(""),
            Gen.Const("   "),
            Gen.Const("/relative/path"),
            Gen.Const("relative/path"),
            Gen.Const("api-is-logs-dev.sdasystems.org/v1/log"),
            Gen.Const("ftp://example.com/x"),
            Gen.Const("file:///etc/hosts"),
            Gen.Const("mailto:someone@example.com"),
            Gen.Const("http://"),
            Gen.Const("https://"),
            Gen.Const("not a url"),
            Gen.String)
            .Where(s => !IsWellFormedHttpUrl(s));

        // Each generated case yields a configure callback that produces exactly one invalid field.
        Gen<Action<LogsOutboxOptions>> invalidConfigure = Gen.OneOf(
            outOfRangeRetry.Select<int, Action<LogsOutboxOptions>>(
                retry => options => options.RetryLimit = retry),
            malformedEndpoint.Select<string, Action<LogsOutboxOptions>>(
                endpoint => options => options.LogBankEndpoint = endpoint));

        invalidConfigure.Sample(configure =>
        {
            // A container pre-seeded with an unrelated registration so we assert on the delta rather
            // than an empty collection.
            var services = new ServiceCollection();
            services.AddSingleton<object>(new object());

            var countBefore = services.Count;

            Assert.Throws<LogsOutboxConfigurationException>(
                () => services.AddLogsOutbox(configure));

            Assert.Equal(countBefore, services.Count);
        }, iter: 100);
    }

    /// <summary>
    /// Mirrors the validator's own well-formedness rule so the generator only feeds genuinely
    /// malformed endpoint strings to the rejection assertion.
    /// </summary>
    private static bool IsWellFormedHttpUrl(string? candidate)
    {
        if (string.IsNullOrWhiteSpace(candidate))
        {
            return false;
        }

        return Uri.TryCreate(candidate, UriKind.Absolute, out var uri)
            && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps);
    }
}
