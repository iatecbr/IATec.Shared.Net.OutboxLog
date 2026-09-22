using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.Extensions.Logging;

namespace IATec.Shared.Net.OutboxLog.Logging;

/// <summary>
/// An <see cref="ILogger"/> that captures log events, builds a <see cref="LogPayload"/> and writes
/// it to the <see cref="IOutboxStore"/>. Every operation is guarded so a logging failure never
/// propagates to the calling application (Req 2.5, 8.2). When the owning provider has been disposed
/// the logger discards events without touching the store (Req 2.6).
/// </summary>
public sealed class OutboxLogger : ILogger
{
    private readonly string _category;
    private readonly IOutboxStore _store;
    private readonly LogPayloadFactory _payloadFactory;
    private readonly LogsOutboxOptions _options;
    private readonly ILogger? _fallbackLogger;
    private readonly Func<bool> _isDisposed;
    private readonly Func<IExternalScopeProvider?> _scopeProvider;

    /// <summary>
    /// Creates a new <see cref="OutboxLogger"/>.
    /// </summary>
    /// <param name="category">The logger category name.</param>
    /// <param name="store">The outbox store the payload is written to.</param>
    /// <param name="payloadFactory">Factory that builds a <see cref="LogPayload"/> from a log event.</param>
    /// <param name="options">Configured context values (container key, source, ...).</param>
    /// <param name="scopeProvider">Accessor for the current external scope provider, if any.</param>
    /// <param name="isDisposed">Predicate that reports whether the owning provider is disposed.</param>
    /// <param name="fallbackLogger">
    /// Optional internal logger used to record swallowed store failures. Never receives payloads
    /// and is never routed back through this provider.
    /// </param>
    public OutboxLogger(
        string category,
        IOutboxStore store,
        LogPayloadFactory payloadFactory,
        LogsOutboxOptions options,
        Func<IExternalScopeProvider?> scopeProvider,
        Func<bool> isDisposed,
        ILogger? fallbackLogger = null)
    {
        _category = category ?? "";
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _payloadFactory = payloadFactory ?? throw new ArgumentNullException(nameof(payloadFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        _isDisposed = isDisposed ?? throw new ArgumentNullException(nameof(isDisposed));
        _fallbackLogger = fallbackLogger;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Non-throwing. Discards the event when the provider is disposed (Req 2.6). Otherwise builds the
    /// payload and writes it to the store, swallowing any failure (Req 2.5, 8.2). The in-memory store
    /// write completes synchronously; the SQL store returns quickly so control returns to the caller
    /// well within the 100 ms budget (Req 2.2). No long async work is awaited here.
    /// </remarks>
    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        // Discard events once the provider is disposed without touching the store.
        if (_isDisposed())
        {
            return;
        }

        if (!IsEnabled(logLevel))
        {
            return;
        }

        try
        {
            var payload = _payloadFactory.Create(
                _category, logLevel, eventId, state, exception, formatter, _scopeProvider(), _options);

            // The write returns control fast: the in-memory store is synchronous and the SQL store
            // returns a quickly-completing task. We observe the outcome without blocking on any
            // long-running async operation. Any fault is swallowed below.
            var writeTask = _store.WriteAsync(payload);

            if (writeTask.IsCompleted)
            {
                // Surface synchronous faults/cancellation into the catch block without blocking.
                _ = writeTask.GetAwaiter().GetResult();
            }
            else
            {
                // Store did not complete synchronously. Do not block the caller; observe the
                // eventual result on a continuation so store faults are still swallowed and never
                // surface as unobserved task exceptions.
                _ = writeTask.ContinueWith(
                    static (t, s) =>
                    {
                        if (t.IsFaulted)
                        {
                            ((OutboxLogger)s!).ReportStoreFailure(t.Exception);
                        }
                    },
                    this,
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted,
                    TaskScheduler.Default);
            }
        }
        catch (Exception ex)
        {
            // Never raise to the calling application. Discard the payload and record the failure
            // through the internal fallback logger, if configured.
            ReportStoreFailure(ex);
        }
    }

    /// <inheritdoc />
    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None && !_isDisposed();

    /// <inheritdoc />
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        => _scopeProvider()?.Push(state);

    private void ReportStoreFailure(Exception? ex)
    {
        if (_fallbackLogger is null || ex is null)
        {
            return;
        }

        try
        {
            _fallbackLogger.LogError(ex, "Failed to write a log payload to the outbox store; the event was discarded.");
        }
        catch
        {
            // The fallback logger must never destabilize the caller either.
        }
    }
}
