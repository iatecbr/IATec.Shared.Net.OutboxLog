namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// A single persisted log record awaiting dispatch. Stored in the Outbox Store until
/// it is delivered to the Log Bank Endpoint or reaches the retry limit.
/// </summary>
public sealed class OutboxEntry
{
    /// <summary>Primary key. Client-generated GUID.</summary>
    public Guid Id { get; set; }

    /// <summary>Deterministic key used to deduplicate log deliveries. Unique across all entries.</summary>
    public string DeduplicationKey { get; set; } = "";

    /// <summary>The log payload. Stored as a JSON column in SQL.</summary>
    public LogPayload Payload { get; set; } = new();

    /// <summary>Current delivery status. Starts as <see cref="DeliveryStatus.Pending"/>.</summary>
    public DeliveryStatus Status { get; set; } = DeliveryStatus.Pending;

    /// <summary>Number of delivery attempts made so far. Starts at zero.</summary>
    public int AttemptCount { get; set; }

    /// <summary>Creation timestamp. Used as the oldest-first ordering key for pending reads.</summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>Earliest time the next delivery attempt may occur (backoff gate).</summary>
    public DateTimeOffset? NextAttemptAt { get; set; }

    /// <summary>Timestamp of successful delivery, if delivered.</summary>
    public DateTimeOffset? DeliveredAt { get; set; }

    /// <summary>Details of the most recent delivery failure, if any.</summary>
    public string? LastError { get; set; }
}
