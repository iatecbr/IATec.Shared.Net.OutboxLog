using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.Net.OutboxLog.Dispatch;
using IATec.Shared.Net.OutboxLog.Logging;
using IATec.Shared.HttpClient.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// Registration extensions for the Logs Outbox library (Requirement 1). Each overload validates the
/// supplied <see cref="LogsOutboxOptions"/> via <see cref="LogsOutboxOptionsValidator"/> <em>before</em>
/// mutating the <see cref="IServiceCollection"/>. When validation fails a
/// <see cref="LogsOutboxConfigurationException"/> is thrown and the service collection is left
/// unmodified.
/// </summary>
public static class LogsOutboxServiceCollectionExtensions
{
    /// <summary>
    /// Registers the logger provider, the selected outbox store, the typed Log Bank client, and the
    /// dispatch worker. This non-generic overload supports only the in-memory store; selecting the
    /// SQL store here is rejected because no consumer <c>DbContext</c> type is supplied (Req 3.8).
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    /// <param name="configure">Callback that populates the <see cref="LogsOutboxOptions"/>.</param>
    /// <returns>The same <paramref name="services"/> instance for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="configure"/> is null.</exception>
    /// <exception cref="LogsOutboxConfigurationException">The resulting options are invalid.</exception>
    public static IServiceCollection AddLogsOutbox(
        this IServiceCollection services,
        Action<LogsOutboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = BuildAndValidate(configure);

        // The non-generic overload cannot bind the SQL store to a consumer DbContext (Req 3.8).
        if (options.StoreType == OutboxStoreType.Sql)
        {
            throw new LogsOutboxConfigurationException(
                "DbContext",
                "The SQL outbox store requires a consumer DbContext type. Use AddLogsOutbox<TDbContext>(...) instead.");
        }

        RegisterCommon(services, options);

        // In-memory store is a singleton with no external dependency.
        services.TryAddSingleton<IOutboxStore>(sp => new InMemoryOutboxStore(timeProvider: null, options: options, serviceProvider: sp));

        // The logger provider is a singleton that writes to the singleton in-memory store.
        // Only registered when the ILogger integration is enabled (opt-out via options).
        if (options.EnableLoggerProvider)
        {
            services.AddSingleton<ILoggerProvider>(sp => new OutboxLoggerProvider(
                sp.GetRequiredService<IOutboxStore>(),
                sp.GetRequiredService<LogPayloadFactory>(),
                sp.GetRequiredService<LogsOutboxOptions>()));
        }

        // Dispatch worker resolves the singleton store directly.
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService>(sp => new DispatchWorker(
            sp.GetRequiredService<IOutboxStore>(),
            sp.GetRequiredService<ILogBankClient>(),
            sp.GetRequiredService<LogsOutboxOptions>(),
            sp.GetService<TimeProvider>(),
            sp.GetService<ILogger<DispatchWorker>>()));

        return services;
    }

    /// <summary>
    /// Registers the logger provider, outbox store, typed client, and dispatch worker. Supports both
    /// the in-memory and SQL stores. When <see cref="LogsOutboxOptions.StoreType"/> is
    /// <see cref="OutboxStoreType.Sql"/> the store is bound to <typeparamref name="TDbContext"/> and a
    /// startup initializer creates the outbox table if missing.
    /// </summary>
    /// <typeparam name="TDbContext">
    /// The consumer-supplied EF Core context used for transactional writes. An
    /// <see cref="IDbContextFactory{TContext}"/> for this type must be registered by the consumer
    /// (for example via <c>AddDbContextFactory&lt;TDbContext&gt;</c>) for the worker's reads/updates.
    /// </typeparam>
    /// <param name="services">The service collection to register into.</param>
    /// <param name="configure">Callback that populates the <see cref="LogsOutboxOptions"/>.</param>
    /// <returns>The same <paramref name="services"/> instance for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="configure"/> is null.</exception>
    /// <exception cref="LogsOutboxConfigurationException">The resulting options are invalid.</exception>
    public static IServiceCollection AddLogsOutbox<TDbContext>(
        this IServiceCollection services,
        Action<LogsOutboxOptions> configure)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = BuildAndValidate(configure);

        RegisterCommon(services, options);

        if (options.StoreType == OutboxStoreType.InMemory)
        {
            // Same in-memory wiring as the non-generic overload.
            services.TryAddSingleton<IOutboxStore>(sp => new InMemoryOutboxStore(timeProvider: null, options: options, serviceProvider: sp));

            if (options.EnableLoggerProvider)
            {
                services.AddSingleton<ILoggerProvider>(sp => new OutboxLoggerProvider(
                    sp.GetRequiredService<IOutboxStore>(),
                    sp.GetRequiredService<LogPayloadFactory>(),
                    sp.GetRequiredService<LogsOutboxOptions>()));
            }

            services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService>(sp => new DispatchWorker(
                sp.GetRequiredService<IOutboxStore>(),
                sp.GetRequiredService<ILogBankClient>(),
                sp.GetRequiredService<LogsOutboxOptions>(),
                sp.GetService<TimeProvider>(),
                sp.GetService<ILogger<DispatchWorker>>()));

            return services;
        }

        // ---- SQL store wiring ----

        // Table initializer (singleton) and a startup hosted service that runs it once.
        services.TryAddSingleton(sp => new SqlOutboxTableInitializer<TDbContext>(
            sp.GetRequiredService<IDbContextFactory<TDbContext>>(),
            tableName: null,
            sp.GetRequiredService<ILogger<SqlOutboxTableInitializer<TDbContext>>>()));

        services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService,
            SqlOutboxTableInitializerHostedService<TDbContext>>();

        // The SQL store is scoped because it needs the scoped, consumer-supplied TDbContext for
        // transactional writes.
        services.TryAddScoped<IOutboxStore>(sp => new SqlOutboxStore<TDbContext>(
            sp.GetRequiredService<TDbContext>(),
            sp.GetRequiredService<IDbContextFactory<TDbContext>>(),
            sp.GetRequiredService<SqlOutboxTableInitializer<TDbContext>>(),
            sp.GetService<TimeProvider>(),
            sp.GetService<ILogger<SqlOutboxStore<TDbContext>>>(),
            sp.GetRequiredService<LogsOutboxOptions>(),
            sp));

        // The logger provider is a singleton, but the SQL store is scoped; the ScopedOutboxStore
        // adapter resolves the scoped store per operation so the singleton provider can write
        // through it. Both are only needed when the ILogger integration is enabled.
        if (options.EnableLoggerProvider)
        {
            services.TryAddSingleton<ScopedOutboxStore>();

            services.AddSingleton<ILoggerProvider>(sp => new OutboxLoggerProvider(
                sp.GetRequiredService<ScopedOutboxStore>(),
                sp.GetRequiredService<LogPayloadFactory>(),
                sp.GetRequiredService<LogsOutboxOptions>()));
        }

        // The worker is a singleton hosted service; it resolves the scoped store from a fresh scope
        // each poll cycle via IServiceScopeFactory.
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService>(sp => new DispatchWorker(
            sp.GetRequiredService<IServiceScopeFactory>(),
            sp.GetRequiredService<ILogBankClient>(),
            sp.GetRequiredService<LogsOutboxOptions>(),
            sp.GetService<TimeProvider>(),
            sp.GetService<ILogger<DispatchWorker>>()));

        return services;
    }

    /// <summary>
    /// Logging-builder overload. Delegates to the <see cref="IServiceCollection"/> overload via
    /// <see cref="ILoggingBuilder.Services"/> and returns the builder for chaining.
    /// </summary>
    /// <param name="builder">The logging builder.</param>
    /// <param name="configure">Callback that populates the <see cref="LogsOutboxOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> instance for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is null.</exception>
    /// <exception cref="LogsOutboxConfigurationException">The resulting options are invalid.</exception>
    public static ILoggingBuilder AddLogsOutbox(
        this ILoggingBuilder builder,
        Action<LogsOutboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddLogsOutbox(configure);
        return builder;
    }

    /// <summary>
    /// Builds a fresh options instance from the callback and validates it. Runs before any
    /// <c>services.Add*</c> call so a rejected configuration leaves the container unmodified.
    /// </summary>
    private static LogsOutboxOptions BuildAndValidate(Action<LogsOutboxOptions> configure)
    {
        var options = new LogsOutboxOptions();
        configure(options);
        LogsOutboxOptionsValidator.Validate(options);
        return options;
    }

    /// <summary>
    /// Registers the store-independent services shared by every overload: the validated options
    /// singleton, the payload factory, and the resilient Log Bank client.
    /// </summary>
    private static void RegisterCommon(IServiceCollection services, LogsOutboxOptions options)
    {
        services.TryAddSingleton(options);
        // The factory receives the application IServiceProvider so LogsOutboxOptions.UserIdProvider
        // can resolve request-scoped services (e.g. IHttpContextAccessor) for the userId fallback.
        services.TryAddSingleton(sp => new LogPayloadFactory(sp));

        RegisterLogBankClient(services, options);
    }

    /// <summary>
    /// Registers the resilient Log Bank client using IATec.Shared.HttpClient. The named client is
    /// configured with a Polly retry/circuit-breaker/timeout policy mapped from the outbox options
    /// so that, when the Log Bank endpoint is unavailable or struggling, the circuit opens and
    /// deliveries are short circuited (mapped to Unreachable) instead of flooding the endpoint.
    /// </summary>
    private static void RegisterLogBankClient(IServiceCollection services, LogsOutboxOptions options)
    {
        var endpoint = new Uri(options.LogBankEndpoint, UriKind.Absolute);
        // Base address is scheme://host:port; the request path is applied by LogBankClient.
        var baseAddress = new Uri(endpoint.GetLeftPart(UriPartial.Authority));

        // IATec.Shared.HttpClient depends on logging and localization (its error responses are
        // localized). Register them here (idempotent) so AddLogsOutbox works on a bare
        // service collection, not only inside a fully configured web host.
        services.AddLogging();
        services.AddLocalization();

        services.AddHttpClientService(
            configurePolicy: policy =>
            {
                policy.UseCircuitBreaker = options.UseCircuitBreaker;
                policy.CircuitBreakerFailuresAllowedBeforeBreaking = options.CircuitBreakerFailuresAllowedBeforeBreaking;
                policy.CircuitBreakerDuration = options.CircuitBreakerDuration;

                policy.UseRetry = options.UseHttpRetry;
                policy.RetryCount = options.HttpRetryCount;
                policy.RetryDelay = options.HttpRetryDelay;

                // Per-attempt HTTP timeout, shared with the outbox option.
                policy.RequestTimeout = options.RequestTimeout;
            },
            configureClient: client =>
            {
                client.BaseAddress = baseAddress;
                client.DefaultRequestHeaders.Add("Accept", "application/json");
            },
            clientName: LogBankClient.LogBankClientName);

        // LogBankClient resolves the named IServiceClient via the factory.
        services.TryAddSingleton<ILogBankClient>(sp => new LogBankClient(
            sp.GetRequiredService<IATec.Shared.HttpClient.Service.IServiceClientFactory>(),
            sp.GetRequiredService<LogsOutboxOptions>()));
    }
}
