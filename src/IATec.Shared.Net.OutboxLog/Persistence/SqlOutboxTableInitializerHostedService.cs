using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// Runs <see cref="SqlOutboxTableInitializer{TDbContext}.InitializeAsync"/> once on application
/// startup so the outbox table is created (if missing) before the dispatch worker begins polling.
/// The initializer never throws on failure — it sets <c>TableAvailable = false</c> and lets startup
/// continue (Req 4.3) — so this hosted service simply awaits it.
/// </summary>
/// <typeparam name="TDbContext">The consumer-supplied EF Core context type.</typeparam>
public sealed class SqlOutboxTableInitializerHostedService<TDbContext> : IHostedService
    where TDbContext : DbContext
{
    private readonly SqlOutboxTableInitializer<TDbContext> _initializer;

    /// <summary>Creates the hosted service over the supplied initializer.</summary>
    /// <param name="initializer">The table initializer to run at startup.</param>
    public SqlOutboxTableInitializerHostedService(SqlOutboxTableInitializer<TDbContext> initializer)
        => _initializer = initializer ?? throw new ArgumentNullException(nameof(initializer));

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
        => _initializer.InitializeAsync(cancellationToken);

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
