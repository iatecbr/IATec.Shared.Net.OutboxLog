namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// The outcome of writing a <see cref="LogPayload"/> to the Outbox Store.
/// </summary>
public enum WriteOutcome
{
    /// <summary>A new outbox entry was persisted.</summary>
    Persisted,

    /// <summary>An entry with the same deduplication key already existed; no new entry was persisted.</summary>
    Deduplicated,

    /// <summary>The write failed; no partial entry was persisted.</summary>
    Failed
}

/// <summary>
/// The result of a write to the Outbox Store, including the outcome, the computed
/// deduplication key, and an optional error description on failure.
/// </summary>
public readonly record struct WriteResult(WriteOutcome Outcome, string DeduplicationKey, string? Error)
{
    /// <summary>
    /// True when the write either persisted a new entry or was treated as a successful deduplication.
    /// </summary>
    public bool Success => Outcome is WriteOutcome.Persisted or WriteOutcome.Deduplicated;
}
