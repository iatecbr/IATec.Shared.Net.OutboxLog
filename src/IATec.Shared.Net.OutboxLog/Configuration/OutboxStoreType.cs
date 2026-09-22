namespace IATec.Shared.Net.OutboxLog.Configuration;

/// <summary>
/// Selects which <c>IOutboxStore</c> implementation the library uses to persist outbox entries.
/// </summary>
public enum OutboxStoreType
{
    /// <summary>Non-durable, process-local store. Suitable for testing and ephemeral workloads.</summary>
    InMemory,

    /// <summary>Durable SQL Server backed store that can participate in ambient transactions.</summary>
    Sql
}
