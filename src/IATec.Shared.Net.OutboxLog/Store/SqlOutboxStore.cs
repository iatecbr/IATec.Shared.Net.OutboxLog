using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// Relational EF Core implementation of <see cref="IOutboxStore"/> (Req 3.3). Provider-agnostic:
/// works with any relational database supported by EF Core (SQL Server, PostgreSQL, MySQL, SQLite,
/// ...). The consumer supplies the <typeparamref name="TDbContext"/> configured with its provider.
/// </summary>
/// <remarks>
/// <para>
/// Writes use the consumer-supplied, scoped <typeparamref name="TDbContext"/> so an
/// <see cref="OutboxEntry"/> produced inside a business transaction commits or rolls back
/// atomically with that transaction (Req 7.1, 7.5). The dispatch worker's reads and status
/// updates use a dedicated context created from an <see cref="IDbContextFactory{TContext}"/>
/// so they never interfere with the consumer's unit of work.
/// </para>
/// <para>
/// Availability is gated by <see cref="SqlOutboxTableInitializer{TDbContext}.TableAvailable"/>:
/// while the table is unavailable, every operation short-circuits without touching the database
/// (Req 4.4). A unique-constraint violation on the deduplication key is mapped to
/// <see cref="WriteOutcome.Deduplicated"/> (Req 6.2).
/// </para>
/// </remarks>
/// <typeparam name="TDbContext">The consumer-supplied EF Core context used for transactional writes.</typeparam>
public sealed class SqlOutboxStore<TDbContext> : IOutboxStore
    where TDbContext : DbContext
{
    private readonly TDbContext _consumerContext;
    private readonly IDbContextFactory<TDbContext> _contextFactory;
    private readonly SqlOutboxTableInitializer<TDbContext> _initializer;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<SqlOutboxStore<TDbContext>>? _logger;
    private readonly LogsOutboxOptions? _options;
    private readonly IServiceProvider? _serviceProvider;

    /// <summary>
    /// Creates the store.
    /// </summary>
    /// <param name="consumerContext">
    /// The scoped, consumer-supplied context used for transactional writes so entries participate
    /// in an active ambient transaction (Req 3.4, 7.1).
    /// </param>
    /// <param name="contextFactory">
    /// Factory used to obtain a dedicated context for the worker's reads and status updates.
    /// </param>
    /// <param name="initializer">Provides the <see cref="IsAvailable"/> gate (Req 4.4).</param>
    /// <param name="timeProvider">
    /// Clock used for <see cref="OutboxEntry.CreatedAt"/> and backoff comparisons. Defaults to
    /// <see cref="TimeProvider.System"/>.
    /// </param>
    /// <param name="logger">Optional logger used to report unexpected persistence failures.</param>
    /// <param name="options">
    /// Global options. When supplied, a direct write whose payload has an empty <c>containerKey</c>
    /// or <c>userId</c> is filled from <see cref="LogsOutboxOptions.ContainerKey"/> and
    /// <see cref="LogsOutboxOptions.UserIdProvider"/> respectively.
    /// </param>
    /// <param name="serviceProvider">Service provider used to invoke the user-id provider, if any.</param>
    public SqlOutboxStore(
        TDbContext consumerContext,
        IDbContextFactory<TDbContext> contextFactory,
        SqlOutboxTableInitializer<TDbContext> initializer,
        TimeProvider? timeProvider = null,
        ILogger<SqlOutboxStore<TDbContext>>? logger = null,
        LogsOutboxOptions? options = null,
        IServiceProvider? serviceProvider = null)
    {
        _consumerContext = consumerContext ?? throw new ArgumentNullException(nameof(consumerContext));
        _contextFactory = contextFactory ?? throw new ArgumentNullException(nameof(contextFactory));
        _initializer = initializer ?? throw new ArgumentNullException(nameof(initializer));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger;
        _options = options;
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public bool IsAvailable => _initializer.TableAvailable;

    /// <inheritdoc />
    public async Task<WriteResult> WriteAsync(LogPayload payload, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ct.ThrowIfCancellationRequested();

        // Fill containerKey/userId from the global configuration when the direct caller left them
        // empty. Done before computing the key so deduplication is consistent.
        payload = OutboxPayloadDefaults.Apply(payload, _options, _serviceProvider);

        var key = DeduplicationKeyGenerator.Compute(payload);

        // Availability guard: reject without touching the database (Req 4.4).
        if (!IsAvailable)
        {
            return new WriteResult(WriteOutcome.Failed, key, "outbox table unavailable");
        }

        var entry = new OutboxEntry
        {
            Id = Guid.NewGuid(),
            DeduplicationKey = key,
            Payload = payload,
            Status = DeliveryStatus.Pending,
            AttemptCount = 0,
            CreatedAt = _timeProvider.GetUtcNow(),
            NextAttemptAt = null,
            DeliveredAt = null,
            LastError = null
        };

        // Detect an ambient transaction on the consumer's unit of work. When one is active the
        // entry is enlisted with the consumer's change tracker and left for the consumer's
        // SaveChanges/commit, so it commits or rolls back atomically with the business data
        // (Req 7.1). When none is active, persist immediately so the worker can pick it up (Req 7.6).
        if (HasAmbientTransaction())
        {
            // Deduplicate BEFORE enlisting the entry in the consumer's change tracker. Adding a
            // duplicate deduplication key would make the consumer's SaveChanges fail with a unique
            // index violation and tear down their business transaction over a mere duplicate log,
            // which must never happen (Req 6.2, 7.5). Check both the local change tracker (an
            // identical log already enlisted in this unit of work) and the database (already
            // persisted by a previous commit), scoped to the consumer's ambient transaction.
            var enlistedLocally = _consumerContext.Set<OutboxEntry>().Local
                .Any(e => e.DeduplicationKey == key);

            var alreadyPersisted = !enlistedLocally
                && await _consumerContext.Set<OutboxEntry>()
                    .AsNoTracking()
                    .AnyAsync(e => e.DeduplicationKey == key, ct)
                    .ConfigureAwait(false);

            if (enlistedLocally || alreadyPersisted)
            {
                return new WriteResult(WriteOutcome.Deduplicated, key, null);
            }

            // Do NOT SaveChanges here: the entry rides the consumer's transaction. Any persistence
            // failure surfaces from the consumer's SaveChanges, preserving the ambient transaction
            // and its business data (Req 7.5).
            _consumerContext.Set<OutboxEntry>().Add(entry);
            return new WriteResult(WriteOutcome.Persisted, key, null);
        }

        // No ambient transaction: persist immediately on a dedicated context so the write is
        // isolated from the consumer's change tracker (Req 7.6).
        await using var context = await _contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);

        // Fast path: if an entry with this deduplication key already exists, treat the write as an
        // idempotent deduplication without attempting an insert. This avoids the common
        // unique-index collision (and its noisy DbUpdateException) when the same log is produced
        // more than once (Req 6.2).
        var alreadyExists = await context.Set<OutboxEntry>()
            .AsNoTracking()
            .AnyAsync(e => e.DeduplicationKey == key, ct)
            .ConfigureAwait(false);

        if (alreadyExists)
        {
            return new WriteResult(WriteOutcome.Deduplicated, key, null);
        }

        try
        {
            context.Set<OutboxEntry>().Add(entry);
            await context.SaveChangesAsync(ct).ConfigureAwait(false);
            return new WriteResult(WriteOutcome.Persisted, key, null);
        }
        catch (DbUpdateException)
        {
            // The insert failed. Because we already checked existence above, the overwhelmingly
            // likely cause is a concurrent writer inserting the same deduplication key between the
            // check and this insert; the unique index is the authoritative guard against duplicates.
            // Treat it as an idempotent deduplication (Req 6.2). This is provider-agnostic: it does
            // not depend on any provider-specific SQL error code.
            return new WriteResult(WriteOutcome.Deduplicated, key, null);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<OutboxEntry>> GetPendingAsync(
        int batchSize,
        int retryLimit,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!IsAvailable || batchSize <= 0)
        {
            return Array.Empty<OutboxEntry>();
        }

        var now = _timeProvider.GetUtcNow();

        await using var context = await _contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);

        var pending = await context.Set<OutboxEntry>()
            .Where(e =>
                e.Status == DeliveryStatus.Pending &&
                e.AttemptCount < retryLimit &&
                (e.NextAttemptAt == null || e.NextAttemptAt <= now))
            .OrderBy(e => e.CreatedAt)
            .Take(batchSize)
            .ToListAsync(ct)
            .ConfigureAwait(false);

        return pending;
    }

    /// <inheritdoc />
    public async Task MarkDeliveredAsync(Guid entryId, CancellationToken ct = default)
    {
        if (!IsAvailable)
        {
            return;
        }

        // A successful delivery removes the entry from the table entirely: once the log bank has
        // accepted the payload there is nothing left to retry or retain, so we delete the row rather
        // than keeping a Delivered tombstone. No-ops when the id is unknown.
        await using var context = await _contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);

        var entry = await context.Set<OutboxEntry>()
            .FirstOrDefaultAsync(e => e.Id == entryId, ct)
            .ConfigureAwait(false);

        if (entry is null)
        {
            return;
        }

        context.Set<OutboxEntry>().Remove(entry);
        await context.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task IncrementAttemptAsync(Guid entryId, DateTimeOffset nextAttemptAt, CancellationToken ct = default)
    {
        if (!IsAvailable)
        {
            return;
        }

        await UpdateEntryAsync(
            entryId,
            entry =>
            {
                entry.AttemptCount++;
                entry.Status = DeliveryStatus.Pending;
                entry.NextAttemptAt = nextAttemptAt;
            },
            ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task MarkFailedAsync(Guid entryId, CancellationToken ct = default)
    {
        if (!IsAvailable)
        {
            return;
        }

        await UpdateEntryAsync(
            entryId,
            entry => entry.Status = DeliveryStatus.Failed,
            ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Loads the entry by id on a dedicated context, applies the mutation, and persists it.
    /// No-ops when the id is unknown. Used by the worker's status transitions.
    /// </summary>
    private async Task UpdateEntryAsync(Guid entryId, Action<OutboxEntry> mutate, CancellationToken ct)
    {
        await using var context = await _contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);

        var entry = await context.Set<OutboxEntry>()
            .FirstOrDefaultAsync(e => e.Id == entryId, ct)
            .ConfigureAwait(false);

        if (entry is null)
        {
            return;
        }

        mutate(entry);
        await context.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Detects an active ambient transaction on the consumer's unit of work: either an EF Core
    /// transaction on the consumer context or an enlisted <see cref="System.Transactions.Transaction"/>.
    /// </summary>
    private bool HasAmbientTransaction()
        => _consumerContext.Database.CurrentTransaction is not null
            || System.Transactions.Transaction.Current is not null;
}
