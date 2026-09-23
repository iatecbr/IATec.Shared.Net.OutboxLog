using System.Collections.Concurrent;
using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.Extensions.Logging;

namespace IATec.Shared.Net.OutboxLog.Logging;

/// <summary>
/// An <see cref="ILoggerProvider"/> that creates <see cref="OutboxLogger"/> instances writing to the
/// configured <see cref="IOutboxStore"/> (Req 2.1). Supports external scopes (Req 2.3) and, on
/// disposal, releases the loggers it created and short-circuits any subsequent writes via a volatile
/// disposed flag (Req 2.6).
/// </summary>
[ProviderAlias("LogsOutbox")]
public sealed class OutboxLoggerProvider : ILoggerProvider, ISupportExternalScope
{
    private readonly IOutboxStore _store;
    private readonly LogPayloadFactory _payloadFactory;
    private readonly LogsOutboxOptions _options;
    private readonly ILogger? _fallbackLogger;
    private readonly ConcurrentDictionary<string, OutboxLogger> _loggers = new(StringComparer.Ordinal);

    private IExternalScopeProvider? _scopeProvider;

    // Volatile so a Dispose on one thread is immediately observed by Log calls on other threads,
    // ensuring subsequent writes short-circuit and discard the event (Req 2.6).
    private volatile bool _disposed;

    /// <summary>
    /// Creates a new <see cref="OutboxLoggerProvider"/>.
    /// </summary>
    /// <param name="store">The outbox store payloads are written to.</param>
    /// <param name="payloadFactory">Factory that builds a <see cref="LogPayload"/> from a log event.</param>
    /// <param name="options">Configured context values applied to every payload.</param>
    /// <param name="fallbackLogger">
    /// Optional internal logger used by the created loggers to record swallowed store failures.
    /// </param>
    public OutboxLoggerProvider(
        IOutboxStore store,
        LogPayloadFactory payloadFactory,
        LogsOutboxOptions options,
        ILogger? fallbackLogger = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _payloadFactory = payloadFactory ?? throw new ArgumentNullException(nameof(payloadFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _fallbackLogger = fallbackLogger;
    }

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName)
    {
        categoryName ??= "";

        return _loggers.GetOrAdd(
            categoryName,
            static (name, self) => new OutboxLogger(
                name,
                self._store,
                self._payloadFactory,
                self._options,
                () => self._scopeProvider,
                () => self._disposed,
                self._fallbackLogger),
            this);
    }

    /// <inheritdoc />
    public void SetScopeProvider(IExternalScopeProvider scopeProvider)
        => _scopeProvider = scopeProvider;

    /// <inheritdoc />
    /// <remarks>
    /// Sets the volatile disposed flag so subsequent <see cref="OutboxLogger.Log{TState}"/> calls
    /// short-circuit, then releases the created loggers (Req 2.6). Idempotent.
    /// </remarks>
    public void Dispose()
    {
        _disposed = true;
        _loggers.Clear();
    }
}
