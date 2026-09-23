using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace IATec.Shared.Net.OutboxLog.Dispatch;

/// <summary>
/// Background service that periodically polls the local Outbox Store for pending entries and
/// delivers them to the Log Bank Endpoint via <see cref="ILogBankClient"/>.
/// </summary>
/// <remarks>
/// <para>
/// Once per <see cref="LogsOutboxOptions.PollInterval"/> the worker fetches a batch of pending
/// entries (<see cref="IOutboxStore.GetPendingAsync"/>) and delivers each one independently:
/// </para>
/// <list type="bullet">
///   <item><description>
///     On <see cref="DeliveryOutcome.Accepted"/> the entry is marked delivered
///     (<see cref="IOutboxStore.MarkDeliveredAsync"/>), retried up to three times if marking fails.
///   </description></item>
///   <item><description>
///     On any non-accepting outcome the attempt count is incremented and a backoff-scheduled
///     <see cref="OutboxEntry.NextAttemptAt"/> is set; once the retry limit is reached the entry is
///     marked failed.
///   </description></item>
/// </list>
/// <para>
/// Per-entry failures are isolated so one bad entry never aborts the batch, and the poll loop is
/// wrapped so an unexpected exception never terminates the worker. On shutdown the worker stops
/// fetching new batches and awaits the in-flight delivery within a 30 second bound; entries that
/// were not delivered remain <see cref="DeliveryStatus.Pending"/> for the next startup.
/// </para>
/// <para>
/// A <see cref="System.TimeProvider"/> drives both the polling cadence and backoff scheduling so
/// timing is deterministic under test.
/// </para>
/// </remarks>
public sealed class DispatchWorker : BackgroundService
{
    /// <summary>Maximum number of attempts to mark an accepted entry as delivered.</summary>
    private const int MarkDeliveredMaxAttempts = 3;

    /// <summary>Upper bound on how long shutdown waits for the in-flight delivery to finish.</summary>
    private static readonly TimeSpan ShutdownDrainTimeout = TimeSpan.FromSeconds(30);

    private readonly IOutboxStore? _store;
    private readonly IServiceScopeFactory? _scopeFactory;
    private readonly ILogBankClient _client;
    private readonly LogsOutboxOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    /// <summary>
    /// Initializes the dispatch worker with a directly injected store. Use this constructor when the
    /// store is a singleton (for example <see cref="InMemoryOutboxStore"/>).
    /// </summary>
    /// <param name="store">The local outbox store to poll and update.</param>
    /// <param name="client">The client used to deliver payloads to the Log Bank Endpoint.</param>
    /// <param name="options">The validated library options (poll interval, batch size, retry limit).</param>
    /// <param name="timeProvider">
    /// Time source driving the poll cadence and backoff scheduling. Defaults to
    /// <see cref="System.TimeProvider.System"/> when not supplied. Tests inject a fake provider for
    /// deterministic timing.
    /// </param>
    /// <param name="logger">
    /// Optional logger for diagnostics. Must not be the library's own outbox provider (to avoid
    /// recursion). Defaults to <see cref="NullLogger{T}"/>.
    /// </param>
    public DispatchWorker(
        IOutboxStore store,
        ILogBankClient client,
        LogsOutboxOptions options,
        TimeProvider? timeProvider = null,
        ILogger<DispatchWorker>? logger = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _scopeFactory = null;
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<DispatchWorker>.Instance;
    }

    /// <summary>
    /// Initializes the dispatch worker with an <see cref="IServiceScopeFactory"/>. Use this
    /// constructor when the store is scoped (for example <see cref="SqlOutboxStore{TDbContext}"/>):
    /// each poll cycle and each status transition resolves a fresh <see cref="IOutboxStore"/> from a
    /// new scope so the scoped store's dependencies (the consumer's DbContext) are honored.
    /// </summary>
    /// <param name="scopeFactory">Factory used to create a scope per cycle and resolve the store.</param>
    /// <param name="client">The client used to deliver payloads to the Log Bank Endpoint.</param>
    /// <param name="options">The validated library options (poll interval, batch size, retry limit).</param>
    /// <param name="timeProvider">
    /// Time source driving the poll cadence and backoff scheduling. Defaults to
    /// <see cref="System.TimeProvider.System"/> when not supplied.
    /// </param>
    /// <param name="logger">Optional logger for diagnostics. Defaults to <see cref="NullLogger{T}"/>.</param>
    public DispatchWorker(
        IServiceScopeFactory scopeFactory,
        ILogBankClient client,
        LogsOutboxOptions options,
        TimeProvider? timeProvider = null,
        ILogger<DispatchWorker>? logger = null)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _store = null;
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<DispatchWorker>.Instance;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunCycleAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Shutdown requested: stop fetching new batches and exit the loop. Any entries not
                // delivered remain Pending and are picked up on the next startup.
                break;
            }
            catch (Exception ex)
            {
                // A cycle-level failure must never terminate the worker; log and resume next cycle.
                _logger.LogError(ex, "Outbox dispatch cycle failed; the worker will continue on the next interval.");
            }

            try
            {
                await Task.Delay(_options.PollInterval, _timeProvider, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Runs exactly one poll cycle synchronously with respect to the caller, for deterministic
    /// testing. Bypasses the background poll loop and the TimeProvider-driven delay so tests can
    /// exercise a single fetch-and-deliver pass without starting the hosted service or racing the
    /// thread-pool scheduler. Uses the same cycle logic as the running worker, so behavior is
    /// identical to production.
    /// </summary>
    internal Task RunSingleCycleForTestsAsync(CancellationToken cancellationToken = default)
        => RunCycleAsync(cancellationToken);

    /// <summary>
    /// Executes a single poll cycle: fetch a pending batch and deliver each entry, isolating
    /// per-entry failures so the batch always runs to completion.
    /// </summary>
    private async Task RunCycleAsync(CancellationToken stoppingToken)
    {
        // When configured with a scope factory the store is scoped: create a fresh scope for the
        // whole cycle and resolve the store from it. Otherwise use the directly injected singleton
        // store. The scope (if any) is disposed at the end of the cycle.
        if (_scopeFactory is not null)
        {
            using IServiceScope scope = _scopeFactory.CreateScope();
            var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
            await RunCycleAsync(store, stoppingToken).ConfigureAwait(false);
        }
        else
        {
            await RunCycleAsync(_store!, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task RunCycleAsync(IOutboxStore store, CancellationToken stoppingToken)
    {
        IReadOnlyList<OutboxEntry> pending =
            await store.GetPendingAsync(_options.BatchSize, _options.RetryLimit, stoppingToken).ConfigureAwait(false);

        foreach (OutboxEntry entry in pending)
        {
            // Once shutdown is requested we stop starting new deliveries; un-dispatched entries stay
            // Pending and are handled on the next startup.
            if (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            try
            {
                await ProcessEntryAsync(store, entry, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Shutdown mid-delivery: leave this entry Pending and stop the batch.
                throw;
            }
            catch (Exception ex)
            {
                // Isolate per-entry failures: log and continue with the rest of the batch.
                _logger.LogError(
                    ex,
                    "Failed to process outbox entry {EntryId}; continuing with the remaining batch.",
                    entry.Id);
            }
        }
    }

    /// <summary>
    /// Delivers a single entry and applies the resulting status transition.
    /// </summary>
    private async Task ProcessEntryAsync(IOutboxStore store, OutboxEntry entry, CancellationToken stoppingToken)
    {
        DeliveryResult result = await _client.SendAsync(entry.Payload, stoppingToken).ConfigureAwait(false);

        if (result.Outcome == DeliveryOutcome.Accepted)
        {
            await MarkDeliveredWithRetryAsync(store, entry, stoppingToken).ConfigureAwait(false);
            return;
        }

        await HandleNonAcceptedAsync(store, entry, result, stoppingToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Marks an accepted entry delivered, retrying up to <see cref="MarkDeliveredMaxAttempts"/>
    /// times. On persistent failure the entry is left in its pre-marking (Pending) state and an
    /// error is logged so it is retried on a later cycle rather than lost or double-counted.
    /// </summary>
    private async Task MarkDeliveredWithRetryAsync(IOutboxStore store, OutboxEntry entry, CancellationToken stoppingToken)
    {
        for (int attempt = 1; attempt <= MarkDeliveredMaxAttempts; attempt++)
        {
            try
            {
                await store.MarkDeliveredAsync(entry.Id, stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Shutdown: stop retrying the marking. The entry stays Pending for next startup.
                throw;
            }
            catch (Exception ex)
            {
                if (attempt == MarkDeliveredMaxAttempts)
                {
                    _logger.LogError(
                        ex,
                        "Failed to mark outbox entry {EntryId} delivered after {Attempts} attempts; " +
                        "the entry is left Pending and will be retried on a later cycle.",
                        entry.Id,
                        MarkDeliveredMaxAttempts);
                }
                else
                {
                    _logger.LogWarning(
                        ex,
                        "Attempt {Attempt} to mark outbox entry {EntryId} delivered failed; retrying.",
                        attempt,
                        entry.Id);
                }
            }
        }
    }

    /// <summary>
    /// Handles a non-accepting delivery outcome: increments the attempt count and either schedules
    /// the next attempt with backoff or marks the entry failed once the retry limit is reached.
    /// </summary>
    private async Task HandleNonAcceptedAsync(IOutboxStore store, OutboxEntry entry, DeliveryResult result, CancellationToken stoppingToken)
    {
        int newAttemptCount = entry.AttemptCount + 1;

        if (newAttemptCount >= _options.RetryLimit)
        {
            await store.MarkFailedAsync(entry.Id, stoppingToken).ConfigureAwait(false);
            _logger.LogWarning(
                "Outbox entry {EntryId} reached the retry limit ({RetryLimit}) after outcome {Outcome} " +
                "(status code {StatusCode}); marked Failed.",
                entry.Id,
                _options.RetryLimit,
                result.Outcome,
                result.StatusCode);
            return;
        }

        DateTimeOffset nextAttemptAt = _timeProvider.GetUtcNow() + RetryBackoffPolicy.ComputeDelay(newAttemptCount);
        await store.IncrementAttemptAsync(entry.Id, nextAttemptAt, stoppingToken).ConfigureAwait(false);
        _logger.LogDebug(
            "Outbox entry {EntryId} delivery not accepted (outcome {Outcome}, status code {StatusCode}); " +
            "attempt {AttemptCount}/{RetryLimit}, next attempt at {NextAttemptAt:o}.",
            entry.Id,
            result.Outcome,
            result.StatusCode,
            newAttemptCount,
            _options.RetryLimit,
            nextAttemptAt);
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal ExecuteAsync to stop fetching new batches, then await the in-flight delivery within
        // the 30s drain bound. Entries not completed remain Pending for the next startup. The drain
        // timer is driven by the injected TimeProvider so shutdown timing is deterministic in tests.
        using var drainCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using ITimer drainTimer = _timeProvider.CreateTimer(
            static state =>
            {
                var cts = (CancellationTokenSource)state!;
                try
                {
                    cts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                    // Shutdown already completed and the source was disposed; nothing to cancel.
                }
            },
            drainCts,
            ShutdownDrainTimeout,
            Timeout.InfiniteTimeSpan);

        try
        {
            await base.StopAsync(drainCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The drain bound elapsed (or the caller cancelled). The un-dispatched entries were never
            // marked, so they remain Pending — nothing to roll back.
            _logger.LogWarning(
                "Outbox dispatch worker did not drain within {DrainSeconds}s of shutdown; " +
                "un-dispatched entries remain Pending for the next startup.",
                ShutdownDrainTimeout.TotalSeconds);
        }
    }
}
