// Feature: logs-outbox-library, Property 16: Transactional atomicity of log production
using System.Diagnostics;
using CsCheck;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.MsSql;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.IntegrationTests;

/// <summary>
/// Integration test for Property 16 (Transactional atomicity of log production), exercised
/// against a real SQL Server instance via Testcontainers.
///
/// Validates: Requirements 7.1, 7.2, 7.3, 7.4, 7.6.
///
/// True commit/rollback visibility semantics cannot be faithfully simulated in memory, so this
/// property is run against a real database. The generated payload sets are pushed through
/// open/commit and open/rollback transaction scopes; "available to the dispatch worker" is
/// modeled by <see cref="SqlOutboxStore{TDbContext}.GetPendingAsync"/>, which reads via the
/// dedicated context factory (the worker's read path).
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class TransactionalAtomicityTests
{
    private const int BatchSize = 1000;
    private const int RetryLimit = 3;

    private readonly SqlServerFixture _fixture;

    public TransactionalAtomicityTests(SqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    [SkippableFact]
    [Trait("Category", "Integration")]
    [Trait("RequiresDocker", "true")]
    public async Task Property16_TransactionalAtomicity_CommitRollbackNoTransaction()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.SkipReason ?? "SQL Server test container is unavailable (Docker not detected).");

        var connectionString = _fixture.ConnectionString!;

        // >=100 iterations over generated payload sets. Each iteration uses a fresh set of
        // payloads with globally-unique content so their deduplication keys never collide
        // across iterations (the store enforces a unique constraint on the dedup key).
        await Gen.Int[1, 6]
            .SelectMany(count => GenPayloadSet(count))
            .SampleAsync(RunAtomicityScenarioAsync, iter: 100, threads: 1);

        return;

        async Task RunAtomicityScenarioAsync(IReadOnlyList<LogPayload> payloads)
        {
            // --- Scenario A: ambient transaction commit (Req 7.1, 7.2, 7.4) ---
            {
                await using var consumerContext = CreateContext(connectionString);
                var store = CreateStore(consumerContext, connectionString);

                await using (var transaction = await consumerContext.Database.BeginTransactionAsync())
                {
                    foreach (var payload in payloads)
                    {
                        var result = await store.WriteAsync(payload);
                        Assert.Equal(WriteOutcome.Persisted, result.Outcome);
                    }

                    // Req 7.1/7.4: entries ride the consumer's uncommitted transaction and are
                    // enlisted with the change tracker only (no SaveChanges yet). The worker's
                    // read path must NOT see them before commit.
                    var keys = payloads.Select(p => DeduplicationKeyGenerator.Compute(p)).ToHashSet();
                    var beforeCommit = await GetPendingKeysAsync(store);
                    Assert.Empty(beforeCommit.Intersect(keys));

                    await consumerContext.SaveChangesAsync();
                    await transaction.CommitAsync();
                }

                // Req 7.2: after commit, all of those entries are available to the worker.
                var committedKeys = payloads.Select(p => DeduplicationKeyGenerator.Compute(p)).ToHashSet();
                var afterCommit = await GetPendingKeysAsync(store);
                Assert.True(committedKeys.IsSubsetOf(afterCommit),
                    "All committed entries must be visible to the dispatch worker after commit.");
            }

            // --- Scenario B: ambient transaction rollback (Req 7.3) ---
            var rollbackPayloads = ReKey(payloads, "rollback");
            {
                await using var consumerContext = CreateContext(connectionString);
                var store = CreateStore(consumerContext, connectionString);

                await using (var transaction = await consumerContext.Database.BeginTransactionAsync())
                {
                    foreach (var payload in rollbackPayloads)
                    {
                        var result = await store.WriteAsync(payload);
                        Assert.Equal(WriteOutcome.Persisted, result.Outcome);
                    }

                    // Persist within the transaction, then roll back. Nothing must survive.
                    await consumerContext.SaveChangesAsync();
                    await transaction.RollbackAsync();
                }

                var rolledBackKeys = rollbackPayloads.Select(p => DeduplicationKeyGenerator.Compute(p)).ToHashSet();
                var afterRollback = await GetPendingKeysAsync(store);
                Assert.Empty(afterRollback.Intersect(rolledBackKeys));
            }

            // --- Scenario C: no ambient transaction (Req 7.6) ---
            var directPayloads = ReKey(payloads, "no-txn");
            {
                await using var consumerContext = CreateContext(connectionString);
                var store = CreateStore(consumerContext, connectionString);

                foreach (var payload in directPayloads)
                {
                    var result = await store.WriteAsync(payload);
                    Assert.Equal(WriteOutcome.Persisted, result.Outcome);
                }

                // Req 7.6: with no ambient transaction, entries persist immediately and are
                // available to the worker without an external commit.
                var directKeys = directPayloads.Select(p => DeduplicationKeyGenerator.Compute(p)).ToHashSet();
                var available = await GetPendingKeysAsync(store);
                Assert.True(directKeys.IsSubsetOf(available),
                    "Entries written with no ambient transaction must be immediately available.");
            }
        }
    }

    /// <summary>
    /// Generates a set of payloads whose content is globally unique (via a monotonic token) so
    /// deduplication keys never collide across iterations or scenarios.
    /// </summary>
    private static Gen<IReadOnlyList<LogPayload>> GenPayloadSet(int count)
        => Gen.Select(
                Gen.String[Gen.Char.AlphaNumeric, 0, 12],
                Gen.String[Gen.Char.AlphaNumeric, 0, 24],
                (source, content) => (source, content))
            .Array[count]
            .Select(parts => (IReadOnlyList<LogPayload>)parts
                .Select(p => new LogPayload
                {
                    ContainerKey = "container",
                    Source = p.source,
                    Owner = "owner",
                    Action = "action",
                    UserId = "user",
                    Content = $"{Guid.NewGuid():N}:{p.content}"
                })
                .ToList());

    /// <summary>
    /// Produces payloads with distinct content (and therefore distinct dedup keys) from the
    /// originals so a rollback/no-transaction scenario never collides with the committed set.
    /// </summary>
    private static IReadOnlyList<LogPayload> ReKey(IReadOnlyList<LogPayload> payloads, string tag)
        => payloads
            .Select(p => p with { Content = $"{tag}:{Guid.NewGuid():N}:{p.Content}" })
            .ToList();

    private static async Task<HashSet<string>> GetPendingKeysAsync(SqlOutboxStore<TestDbContext> store)
    {
        var pending = await store.GetPendingAsync(BatchSize, RetryLimit);
        return pending.Select(e => e.DeduplicationKey).ToHashSet();
    }

    private static TestDbContext CreateContext(string connectionString)
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlServer(connectionString)
            .Options;
        return new TestDbContext(options);
    }

    private static SqlOutboxStore<TestDbContext> CreateStore(TestDbContext consumerContext, string connectionString)
    {
        var factory = new TestDbContextFactory(connectionString);
        var initializer = new SqlOutboxTableInitializer<TestDbContext>(
            factory,
            OutboxEntryConfiguration.DefaultTableName,
            NullLogger<SqlOutboxTableInitializer<TestDbContext>>.Instance);

        // The table is created once per container in the fixture, so InitializeAsync here just
        // confirms availability (idempotent IF OBJECT_ID guard).
        initializer.InitializeAsync(CancellationToken.None).GetAwaiter().GetResult();

        return new SqlOutboxStore<TestDbContext>(
            consumerContext,
            factory,
            initializer);
    }
}

/// <summary>
/// Minimal EF Core context that maps <see cref="OutboxEntry"/> using the library's
/// <see cref="OutboxEntryConfiguration"/> so the schema matches production exactly.
/// </summary>
public sealed class TestDbContext : DbContext
{
    public TestDbContext(DbContextOptions<TestDbContext> options)
        : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration());
    }
}

/// <summary>
/// Simple <see cref="IDbContextFactory{TContext}"/> that hands out fresh contexts bound to the
/// container connection string, used for the worker's dedicated read/DDL contexts.
/// </summary>
public sealed class TestDbContextFactory : IDbContextFactory<TestDbContext>
{
    private readonly string _connectionString;

    public TestDbContextFactory(string connectionString)
    {
        _connectionString = connectionString;
    }

    public TestDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlServer(_connectionString)
            .Options;
        return new TestDbContext(options);
    }
}
