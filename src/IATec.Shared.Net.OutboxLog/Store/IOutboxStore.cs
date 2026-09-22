namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// Abstraction over the local Outbox Store. Isolates the logger and dispatch worker
/// from the persistence choice (in-memory or SQL Server).
/// </summary>
public interface IOutboxStore
{
    /// <summary>
    /// Persists a pending <see cref="OutboxEntry"/> for the payload. Deterministically computes the
    /// deduplication key. If an entry with the same key exists, returns
    /// <see cref="WriteOutcome.Deduplicated"/> without persisting a new entry. Never persists a
    /// partial entry on failure. For the SQL store, participates in the ambient transaction when
    /// one is active.
    /// </summary>
    Task<WriteResult> WriteAsync(LogPayload payload, CancellationToken ct = default);

    /// <summary>
    /// Returns up to <paramref name="batchSize"/> pending entries with attempts &lt;
    /// <paramref name="retryLimit"/> whose backoff has elapsed, ordered oldest-to-newest by
    /// <see cref="OutboxEntry.CreatedAt"/>.
    /// </summary>
    Task<IReadOnlyList<OutboxEntry>> GetPendingAsync(int batchSize, int retryLimit, CancellationToken ct = default);

    /// <summary>Removes the entry from the store after a successful delivery. No-ops when the id is unknown.</summary>
    Task MarkDeliveredAsync(Guid entryId, CancellationToken ct = default);

    /// <summary>Increments AttemptCount, keeps status Pending, sets NextAttemptAt from backoff.</summary>
    Task IncrementAttemptAsync(Guid entryId, DateTimeOffset nextAttemptAt, CancellationToken ct = default);

    /// <summary>Marks the entry failed after the retry limit is reached.</summary>
    Task MarkFailedAsync(Guid entryId, CancellationToken ct = default);

    /// <summary>False when the SQL table creation failed and the store is unavailable.</summary>
    bool IsAvailable { get; }
}
