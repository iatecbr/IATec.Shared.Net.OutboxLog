// Feature: logs-outbox-library
// Task 10.6: SQL integration tests for table lifecycle and transactional errors.
// Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5, 7.5.
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.IntegrationTests;

/// <summary>
/// Integration tests for the SQL outbox table lifecycle (silent auto-creation, idempotent
/// re-initialization, unavailable-on-failure, concurrent creation) and for transactional
/// persistence errors, exercised against a real SQL Server instance via Testcontainers.
///
/// Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5, 7.5.
///
/// Each test targets a UNIQUE table name in the shared container so lifecycle scenarios (which
/// need a table that does or does not yet exist) stay isolated from one another and from the
/// fixture's default table. If Docker is unavailable the tests skip cleanly.
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class TableLifecycleTests
{
    private readonly SqlServerFixture _fixture;

    public TableLifecycleTests(SqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>
    /// Req 4.1: On a database where the outbox table is missing, initialization creates it,
    /// reports <see cref="SqlOutboxTableInitializer{TDbContext}.TableAvailable"/> as <c>true</c>,
    /// and the table is present in the database afterwards.
    /// </summary>
    [SkippableFact]
    [Trait("Category", "Integration")]
    [Trait("RequiresDocker", "true")]
    public async Task InitializeAsync_TableMissing_CreatesTableAndReportsAvailable()
    {
        Skip.IfNot(_fixture.IsAvailable, SkipMessage);

        var connectionString = _fixture.ConnectionString!;
        var tableName = NewTableName();

        Assert.False(await TableExistsAsync(connectionString, tableName));

        var initializer = CreateInitializer(connectionString, tableName, NullLogger<SqlOutboxTableInitializer<NamedTableDbContext>>.Instance);
        await initializer.InitializeAsync(CancellationToken.None);

        Assert.True(initializer.TableAvailable);
        Assert.True(await TableExistsAsync(connectionString, tableName));
    }

    /// <summary>
    /// Req 4.2: A second initialization against an existing, populated table leaves the table and
    /// all its rows intact. Rows are written through <see cref="SqlOutboxStore{TDbContext}"/> with
    /// no ambient transaction.
    /// </summary>
    [SkippableFact]
    [Trait("Category", "Integration")]
    [Trait("RequiresDocker", "true")]
    public async Task InitializeAsync_TableExistsWithRows_PreservesTableAndRows()
    {
        Skip.IfNot(_fixture.IsAvailable, SkipMessage);

        var connectionString = _fixture.ConnectionString!;
        var tableName = NewTableName();

        // First initialization creates the table.
        var firstInit = CreateInitializer(connectionString, tableName, NullLogger<SqlOutboxTableInitializer<NamedTableDbContext>>.Instance);
        await firstInit.InitializeAsync(CancellationToken.None);
        Assert.True(firstInit.TableAvailable);

        // Insert rows via the store (no ambient transaction => immediate persistence, Req 7.6).
        var payloads = Enumerable.Range(0, 5)
            .Select(i => NewPayload($"preserve-{i}"))
            .ToList();

        await using (var consumerContext = CreateContext(connectionString, tableName))
        {
            var store = CreateStore(consumerContext, connectionString, tableName, firstInit);
            foreach (var payload in payloads)
            {
                var result = await store.WriteAsync(payload);
                Assert.Equal(WriteOutcome.Persisted, result.Outcome);
            }
        }

        var rowsBefore = await RowCountAsync(connectionString, tableName);
        Assert.Equal(payloads.Count, rowsBefore);

        // SECOND initialization: idempotent IF OBJECT_ID guard must leave everything untouched.
        var secondInit = CreateInitializer(connectionString, tableName, NullLogger<SqlOutboxTableInitializer<NamedTableDbContext>>.Instance);
        await secondInit.InitializeAsync(CancellationToken.None);

        Assert.True(secondInit.TableAvailable);
        Assert.True(await TableExistsAsync(connectionString, tableName));

        var rowsAfter = await RowCountAsync(connectionString, tableName);
        Assert.Equal(rowsBefore, rowsAfter);

        // The specific rows are still present and readable.
        var pending = await GetPendingViaStoreAsync(connectionString, tableName, secondInit);
        var expectedKeys = payloads.Select(p => DeduplicationKeyGenerator.Compute(p)).ToHashSet();
        Assert.True(expectedKeys.IsSubsetOf(pending), "All rows written before the second init must survive it.");
    }

    /// <summary>
    /// Req 4.3 / 4.4: Pointed at an unreachable database, initialization does NOT throw, logs an
    /// error (captured via a spy logger), reports the table unavailable, and the store then rejects
    /// operations with <see cref="SqlOutboxStore{TDbContext}.IsAvailable"/> == <c>false</c>.
    /// </summary>
    [SkippableFact]
    [Trait("Category", "Integration")]
    [Trait("RequiresDocker", "true")]
    public async Task InitializeAsync_UnreachableDatabase_LogsDoesNotThrowAndStoreRejects()
    {
        Skip.IfNot(_fixture.IsAvailable, SkipMessage);

        // A syntactically valid but unroutable connection string. A short connect timeout keeps the
        // attempt well within the initializer's 30s bound.
        const string unreachable =
            "Server=tcp:10.255.255.1,14330;Database=Nonexistent;User Id=sa;Password=Nope_12345;" +
            "Connect Timeout=3;TrustServerCertificate=True;Encrypt=False";

        var tableName = NewTableName();
        var spyLogger = new SpyLogger<SqlOutboxTableInitializer<NamedTableDbContext>>();

        var initializer = CreateInitializer(unreachable, tableName, spyLogger);

        // Must NOT throw even though the database is unreachable (Req 4.3).
        var initTask = initializer.InitializeAsync(CancellationToken.None);
        await initTask; // no exception expected

        Assert.False(initializer.TableAvailable);
        Assert.True(spyLogger.HasError, "A creation failure must be logged at error level (Req 4.3).");

        // The store must then report unavailable and reject operations without touching the DB (Req 4.4).
        await using var consumerContext = CreateContext(unreachable, tableName);
        var store = new SqlOutboxStore<NamedTableDbContext>(
            consumerContext,
            new NamedTableDbContextFactory(unreachable, tableName),
            initializer);

        Assert.False(store.IsAvailable);

        var writeResult = await store.WriteAsync(NewPayload("rejected"));
        Assert.Equal(WriteOutcome.Failed, writeResult.Outcome);
        Assert.False(writeResult.Success);

        var pending = await store.GetPendingAsync(100, 3);
        Assert.Empty(pending);
    }

    /// <summary>
    /// Req 4.5: N initializer instances racing to create the same missing table produce exactly one
    /// table, all report available, and none crash (a concurrent-creation conflict is treated as an
    /// existing table).
    /// </summary>
    [SkippableFact]
    [Trait("Category", "Integration")]
    [Trait("RequiresDocker", "true")]
    public async Task InitializeAsync_ConcurrentInitializers_YieldExactlyOneTableWithoutCrash()
    {
        Skip.IfNot(_fixture.IsAvailable, SkipMessage);

        var connectionString = _fixture.ConnectionString!;
        var tableName = NewTableName();

        Assert.False(await TableExistsAsync(connectionString, tableName));

        const int instanceCount = 6;
        var initializers = Enumerable.Range(0, instanceCount)
            .Select(_ => CreateInitializer(connectionString, tableName, NullLogger<SqlOutboxTableInitializer<NamedTableDbContext>>.Instance))
            .ToList();

        // Kick them all off together so they race on the same fresh table.
        var tasks = initializers
            .Select(init => Task.Run(() => init.InitializeAsync(CancellationToken.None)))
            .ToArray();

        await Task.WhenAll(tasks); // must not crash

        Assert.All(initializers, init => Assert.True(init.TableAvailable,
            "Every concurrent initializer must report the table available (Req 4.5)."));

        Assert.Equal(1, await TableObjectCountAsync(connectionString, tableName));
    }

    /// <summary>
    /// Req 6.2, 7.1, 7.5: Writing a duplicate deduplication key inside an ambient transaction is
    /// deduplicated BEFORE it is enlisted in the consumer's change tracker, so it never makes the
    /// consumer's SaveChanges fail. A distinct business outbox row written in the same transaction
    /// commits atomically, and the pre-existing row for the duplicated key remains a single row.
    /// This proves a duplicate log can never tear down the consumer's business transaction.
    /// </summary>
    [SkippableFact]
    [Trait("Category", "Integration")]
    [Trait("RequiresDocker", "true")]
    public async Task WriteWithinTransaction_DuplicateKey_IsDeduplicated_AndPreservesBusinessTransaction()
    {
        Skip.IfNot(_fixture.IsAvailable, SkipMessage);

        var connectionString = _fixture.ConnectionString!;
        var tableName = NewTableName();

        var initializer = CreateInitializer(connectionString, tableName, NullLogger<SqlOutboxTableInitializer<NamedTableDbContext>>.Instance);
        await initializer.InitializeAsync(CancellationToken.None);
        Assert.True(initializer.TableAvailable);

        // A payload already committed as a row (no ambient transaction) so a later write of the SAME
        // dedup key inside a transaction would previously have collided on the unique index.
        var duplicated = NewPayload("dup-key");
        await using (var seedContext = CreateContext(connectionString, tableName))
        {
            var seedStore = CreateStore(seedContext, connectionString, tableName, initializer);
            var seedResult = await seedStore.WriteAsync(duplicated);
            Assert.Equal(WriteOutcome.Persisted, seedResult.Outcome);
        }

        var rowsBefore = await RowCountAsync(connectionString, tableName);
        Assert.Equal(1, rowsBefore);

        await using var consumerContext = CreateContext(connectionString, tableName);
        var store = CreateStore(consumerContext, connectionString, tableName, initializer);

        await using var transaction = await consumerContext.Database.BeginTransactionAsync();

        // A distinct "business" outbox row written inside the transaction. With an ambient
        // transaction active, WriteAsync enlists it with the change tracker (no SaveChanges) so it
        // rides the consumer's unit of work (Req 7.1).
        var businessPayload = NewPayload("business-row");
        var businessWrite = await store.WriteAsync(businessPayload);
        Assert.Equal(WriteOutcome.Persisted, businessWrite.Outcome);

        // Writing the duplicate is deduplicated BEFORE enlistment: it is NOT added to the change
        // tracker, so the consumer's SaveChanges will not hit a unique-index violation (Req 6.2, 7.5).
        var duplicateWrite = await store.WriteAsync(duplicated);
        Assert.Equal(WriteOutcome.Deduplicated, duplicateWrite.Outcome);

        // SaveChanges succeeds (only the business row is pending) and the transaction commits
        // cleanly: a duplicate log never tears down the consumer's business transaction.
        var saved = await consumerContext.SaveChangesAsync();
        Assert.Equal(1, saved);
        await transaction.CommitAsync();

        // The business row was committed, and the duplicated key still has exactly one row.
        var businessKey = DeduplicationKeyGenerator.Compute(businessPayload);
        Assert.Equal(1, await RowCountForKeyAsync(connectionString, tableName, businessKey));

        var duplicatedKey = DeduplicationKeyGenerator.Compute(duplicated);
        Assert.Equal(1, await RowCountForKeyAsync(connectionString, tableName, duplicatedKey));

        // Two distinct rows in total: the seeded duplicate + the committed business row.
        var rowsAfter = await RowCountAsync(connectionString, tableName);
        Assert.Equal(2, rowsAfter);
    }

    // ---- helpers -----------------------------------------------------------------------------

    private const string SkipMessage = "SQL Server test container is unavailable (Docker not detected).";

    private static string NewTableName()
        => $"LogsOutboxLifecycle_{Guid.NewGuid():N}";

    private static LogPayload NewPayload(string tag)
        => new()
        {
            ContainerKey = "container",
            Source = "source",
            Owner = "owner",
            Action = "action",
            UserId = "user",
            Content = $"{tag}:{Guid.NewGuid():N}"
        };

    private static SqlOutboxTableInitializer<NamedTableDbContext> CreateInitializer(
        string connectionString,
        string tableName,
        ILogger<SqlOutboxTableInitializer<NamedTableDbContext>> logger)
        => new(new NamedTableDbContextFactory(connectionString, tableName), tableName, logger);

    private static NamedTableDbContext CreateContext(string connectionString, string tableName)
    {
        var options = new DbContextOptionsBuilder<NamedTableDbContext>()
            .UseSqlServer(connectionString)
            .Options;
        return new NamedTableDbContext(options, tableName);
    }

    private static SqlOutboxStore<NamedTableDbContext> CreateStore(
        NamedTableDbContext consumerContext,
        string connectionString,
        string tableName,
        SqlOutboxTableInitializer<NamedTableDbContext> initializer)
        => new(
            consumerContext,
            new NamedTableDbContextFactory(connectionString, tableName),
            initializer);

    private static async Task<HashSet<string>> GetPendingViaStoreAsync(
        string connectionString,
        string tableName,
        SqlOutboxTableInitializer<NamedTableDbContext> initializer)
    {
        await using var consumerContext = CreateContext(connectionString, tableName);
        var store = CreateStore(consumerContext, connectionString, tableName, initializer);
        var pending = await store.GetPendingAsync(1000, 3);
        return pending.Select(e => e.DeduplicationKey).ToHashSet();
    }

    private static async Task<bool> TableExistsAsync(string connectionString, string tableName)
        => await TableObjectCountAsync(connectionString, tableName) > 0;

    private static async Task<int> TableObjectCountAsync(string connectionString, string tableName)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sys.tables WHERE name = @name";
        command.Parameters.AddWithValue("@name", tableName);
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private static async Task<int> RowCountAsync(string connectionString, string tableName)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM [{EscapeIdentifier(tableName)}]";
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private static async Task<int> RowCountForKeyAsync(string connectionString, string tableName, string dedupKey)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT COUNT(*) FROM [{EscapeIdentifier(tableName)}] WHERE [DeduplicationKey] = @key";
        command.Parameters.AddWithValue("@key", dedupKey);
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private static string EscapeIdentifier(string identifier)
        => identifier.Replace("]", "]]", StringComparison.Ordinal);
}

/// <summary>
/// EF Core context that maps <see cref="OutboxEntry"/> to a caller-supplied table name using the
/// library's <see cref="OutboxEntryConfiguration"/>, so each lifecycle test can use an isolated
/// table within the shared container.
/// </summary>
public sealed class NamedTableDbContext : DbContext
{
    private readonly string _tableName;

    public NamedTableDbContext(DbContextOptions<NamedTableDbContext> options, string tableName)
        : base(options)
    {
        _tableName = tableName;
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration(_tableName));
    }
}

/// <summary>
/// <see cref="IDbContextFactory{TContext}"/> that produces <see cref="NamedTableDbContext"/>
/// instances bound to a fixed connection string and table name.
/// </summary>
public sealed class NamedTableDbContextFactory : IDbContextFactory<NamedTableDbContext>
{
    private readonly string _connectionString;
    private readonly string _tableName;

    public NamedTableDbContextFactory(string connectionString, string tableName)
    {
        _connectionString = connectionString;
        _tableName = tableName;
    }

    public NamedTableDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<NamedTableDbContext>()
            .UseSqlServer(_connectionString)
            .Options;
        return new NamedTableDbContext(options, _tableName);
    }
}

/// <summary>
/// Minimal spy <see cref="ILogger{T}"/> that records whether an error-level entry was logged,
/// used to assert the initializer logs a creation failure (Req 4.3) without failing the caller.
/// </summary>
public sealed class SpyLogger<T> : ILogger<T>
{
    private volatile bool _errorLogged;

    public bool HasError => _errorLogged;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (logLevel >= LogLevel.Error)
        {
            _errorLogged = true;
        }
    }
}
