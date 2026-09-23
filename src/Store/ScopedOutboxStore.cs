using Microsoft.Extensions.DependencyInjection;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// A singleton <see cref="IOutboxStore"/> adapter that forwards every operation to a scoped inner
/// store (for example <see cref="SqlOutboxStore{TDbContext}"/>) resolved from a freshly created
/// service scope. This lets singleton consumers (the <c>OutboxLoggerProvider</c>) use a scoped
/// store implementation whose dependencies (such as a consumer-supplied <c>DbContext</c>) live in a
/// scope.
/// </summary>
/// <remarks>
/// Each call creates and disposes its own scope so the inner store's scoped dependencies are
/// materialized and released per operation. Ambient <see cref="System.Transactions.Transaction"/>
/// scopes flow across the created scope because they are thread/execution-context ambient, so a
/// consumer's <c>TransactionScope</c> is still observed by the SQL store's ambient-transaction
/// detection.
/// </remarks>
public sealed class ScopedOutboxStore : IOutboxStore
{
    private readonly IServiceScopeFactory _scopeFactory;

    /// <summary>Creates the adapter over the supplied scope factory.</summary>
    /// <param name="scopeFactory">Factory used to create a scope per operation.</param>
    public ScopedOutboxStore(IServiceScopeFactory scopeFactory)
        => _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));

    /// <inheritdoc />
    /// <remarks>
    /// Availability is resolved on demand from a transient scope; the store is considered available
    /// when a freshly resolved inner store reports availability.
    /// </remarks>
    public bool IsAvailable
    {
        get
        {
            using IServiceScope scope = _scopeFactory.CreateScope();
            return scope.ServiceProvider.GetRequiredService<IOutboxStore>().IsAvailable;
        }
    }

    /// <inheritdoc />
    public async Task<WriteResult> WriteAsync(LogPayload payload, CancellationToken ct = default)
    {
        using IServiceScope scope = _scopeFactory.CreateScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        return await store.WriteAsync(payload, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<OutboxEntry>> GetPendingAsync(int batchSize, int retryLimit, CancellationToken ct = default)
    {
        using IServiceScope scope = _scopeFactory.CreateScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        return await store.GetPendingAsync(batchSize, retryLimit, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task MarkDeliveredAsync(Guid entryId, CancellationToken ct = default)
    {
        using IServiceScope scope = _scopeFactory.CreateScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        await store.MarkDeliveredAsync(entryId, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task IncrementAttemptAsync(Guid entryId, DateTimeOffset nextAttemptAt, CancellationToken ct = default)
    {
        using IServiceScope scope = _scopeFactory.CreateScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        await store.IncrementAttemptAsync(entryId, nextAttemptAt, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task MarkFailedAsync(Guid entryId, CancellationToken ct = default)
    {
        using IServiceScope scope = _scopeFactory.CreateScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        await store.MarkFailedAsync(entryId, ct).ConfigureAwait(false);
    }
}
