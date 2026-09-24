namespace IATec.Shared.Net.OutboxLog.Dispatch;

/// <summary>
/// Convenience service that builds a <see cref="LogPayload"/> from primitive arguments and writes it
/// to the <see cref="IOutboxStore"/>. It is a thin, opinionated wrapper over
/// <see cref="IOutboxStore.WriteAsync(LogPayload, System.Threading.CancellationToken)"/> intended for
/// application code that wants to emit a log without constructing the payload by hand.
/// </summary>
/// <remarks>
/// Registered by default as <c>Scoped</c> by <c>AddLogsOutbox</c>, so it is compatible with both the
/// singleton in-memory store and the scoped SQL store (which requires the consumer's scoped
/// <c>DbContext</c> for transactional writes). <c>containerKey</c> and <c>userId</c> are filled from
/// the global <c>LogsOutboxOptions</c> by the store, so callers do not need to supply them here.
/// </remarks>
public interface ILogDispatcher
{
    /// <summary>
    /// Builds a payload and persists it to the outbox for asynchronous delivery to the Log Bank.
    /// </summary>
    /// <param name="source">
    /// Origin of the log event. When null, empty, or whitespace, it falls back to the entry
    /// assembly name (and to <c>"unknown"</c> when the entry assembly cannot be resolved).
    /// </param>
    /// <param name="owner">Owner associated with the log event. Required (non-empty).</param>
    /// <param name="action">Action associated with the log event. Required (non-empty).</param>
    /// <param name="content">
    /// Optional content. A null value produces an empty string; a <see cref="string"/> is used
    /// verbatim; any other object is serialized to JSON.
    /// </param>
    /// <param name="cancellationToken">A token linked to the caller's lifetime.</param>
    /// <returns>
    /// The <see cref="WriteResult"/> describing the outcome (<c>Persisted</c>, <c>Deduplicated</c>,
    /// or <c>Failed</c>).
    /// </returns>
    /// <exception cref="System.ArgumentException"><paramref name="owner"/> or <paramref name="action"/> is null, empty, or whitespace.</exception>
    Task<WriteResult> DispatchAsync(
        string? source,
        string owner,
        string action,
        object? content = null,
        CancellationToken cancellationToken = default);
}
