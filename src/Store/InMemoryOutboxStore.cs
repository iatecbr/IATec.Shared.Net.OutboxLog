using System.Collections.Concurrent;
using IATec.Shared.Net.OutboxLog.Configuration;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// In-memory reference implementation of <see cref="IOutboxStore"/>.
/// </summary>
/// <remarks>
/// Backed by a <see cref="ConcurrentDictionary{TKey,TValue}"/> keyed by
/// <see cref="OutboxEntry.DeduplicationKey"/> for O(1) deduplication. A monotonic sequence
/// counter records insertion order so pending reads can be returned oldest-first with a stable
/// tiebreak when several entries share the same <see cref="OutboxEntry.CreatedAt"/>. All state
/// transitions use atomic dictionary operations so no caller ever observes a partially written
/// entry. <see cref="IsAvailable"/> is always <see langword="true"/> since there is no external
/// dependency that can become unavailable.
/// </remarks>
public sealed class InMemoryOutboxStore : IOutboxStore
{
    private readonly ConcurrentDictionary<string, StoredEntry> _entries = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Guid, string> _keysById = new();
    private readonly TimeProvider _timeProvider;
    private readonly LogsOutboxOptions? _options;
    private readonly IServiceProvider? _serviceProvider;
    private long _sequence;

    /// <summary>
    /// Creates a new in-memory store.
    /// </summary>
    /// <param name="timeProvider">
    /// Clock used for <see cref="OutboxEntry.CreatedAt"/> and backoff comparisons. Defaults to
    /// <see cref="TimeProvider.System"/>. Inject a fake provider for deterministic tests.
    /// </param>
    /// <param name="options">
    /// Global options. When supplied, a direct write whose payload has an empty <c>containerKey</c>
    /// or <c>userId</c> is filled from <see cref="LogsOutboxOptions.ContainerKey"/> and
    /// <see cref="LogsOutboxOptions.UserIdProvider"/> respectively.
    /// </param>
    /// <param name="serviceProvider">Service provider used to invoke the user-id provider, if any.</param>
    public InMemoryOutboxStore(
        TimeProvider? timeProvider = null,
        LogsOutboxOptions? options = null,
        IServiceProvider? serviceProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _options = options;
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public bool IsAvailable => true;

    /// <inheritdoc />
    public Task<WriteResult> WriteAsync(LogPayload payload, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ct.ThrowIfCancellationRequested();

        // Fill containerKey/userId from the global configuration when the direct caller left them
        // empty. Done before computing the key so deduplication is consistent.
        payload = OutboxPayloadDefaults.Apply(payload, _options, _serviceProvider);

        var key = DeduplicationKeyGenerator.Compute(payload);

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

        var sequence = Interlocked.Increment(ref _sequence);
        var stored = new StoredEntry(entry, sequence);

        // TryAdd is atomic: it either inserts the new entry (Persisted) or leaves the existing
        // one untouched (Deduplicated). No partial state is ever observable.
        if (_entries.TryAdd(key, stored))
        {
            _keysById[entry.Id] = key;
            return Task.FromResult(new WriteResult(WriteOutcome.Persisted, key, null));
        }

        return Task.FromResult(new WriteResult(WriteOutcome.Deduplicated, key, null));
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<OutboxEntry>> GetPendingAsync(int batchSize, int retryLimit, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (batchSize <= 0)
        {
            return Task.FromResult<IReadOnlyList<OutboxEntry>>(Array.Empty<OutboxEntry>());
        }

        var now = _timeProvider.GetUtcNow();

        // Snapshot then filter, order oldest-first by CreatedAt (sequence as stable tiebreak),
        // and cap at batchSize. Iterating a ConcurrentDictionary yields a consistent snapshot.
        var pending = _entries.Values
            .Where(stored =>
                stored.Entry.Status == DeliveryStatus.Pending &&
                stored.Entry.AttemptCount < retryLimit &&
                (stored.Entry.NextAttemptAt is null || stored.Entry.NextAttemptAt.Value <= now))
            .OrderBy(stored => stored.Entry.CreatedAt)
            .ThenBy(stored => stored.Sequence)
            .Take(batchSize)
            .Select(stored => stored.Entry)
            .ToList();

        return Task.FromResult<IReadOnlyList<OutboxEntry>>(pending);
    }

    /// <inheritdoc />
    public Task MarkDeliveredAsync(Guid entryId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        // A successful delivery removes the entry from the store entirely: once the log bank has
        // accepted the payload there is nothing left to retry or retain, so we free the memory
        // rather than keeping a Delivered tombstone.
        if (_keysById.TryRemove(entryId, out var key))
        {
            _entries.TryRemove(key, out _);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task IncrementAttemptAsync(Guid entryId, DateTimeOffset nextAttemptAt, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        UpdateEntry(entryId, entry =>
        {
            entry.AttemptCount++;
            entry.Status = DeliveryStatus.Pending;
            entry.NextAttemptAt = nextAttemptAt;
        });

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task MarkFailedAsync(Guid entryId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        UpdateEntry(entryId, entry => entry.Status = DeliveryStatus.Failed);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Applies an atomic mutation to the entry with the supplied id. Uses
    /// <see cref="ConcurrentDictionary{TKey,TValue}.AddOrUpdate(TKey,Func{TKey,TValue},Func{TKey,TValue,TValue})"/>
    /// so the transition is a single atomic dictionary operation. No-ops if the id is unknown.
    /// </summary>
    private void UpdateEntry(Guid entryId, Action<OutboxEntry> mutate)
    {
        if (!_keysById.TryGetValue(entryId, out var key))
        {
            return;
        }

        _entries.AddOrUpdate(
            key,
            static (_, _) => throw new InvalidOperationException("Cannot add a missing entry during update."),
            (_, existing, apply) =>
            {
                apply(existing.Entry);
                return existing;
            },
            mutate);
    }

    /// <summary>
    /// Wraps an <see cref="OutboxEntry"/> with its monotonic insertion sequence to provide a
    /// stable oldest-first tiebreak independent of the wall clock.
    /// </summary>
    private sealed record StoredEntry(OutboxEntry Entry, long Sequence);
}
