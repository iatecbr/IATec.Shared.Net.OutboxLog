using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// Ensures the outbox table exists in the target relational database, using EF Core so it works
/// with any provider supported by EF Core (SQL Server, PostgreSQL, MySQL, SQLite, ...). Provider
/// agnostic: EF generates the correct DDL for the active provider (Req 4.1).
/// </summary>
/// <remarks>
/// <para>
/// Creates ONLY the outbox table, so it is safe to point <typeparamref name="TDbContext"/> at a
/// context that already owns other (business) tables managed by migrations. The strategy is:
/// </para>
/// <list type="number">
///   <item><description>If the outbox table is already queryable, do nothing (Req 4.2).</description></item>
///   <item><description>Otherwise, generate the model's create script via
///   <see cref="IRelationalDatabaseCreator.GenerateCreateScript"/> (provider-correct DDL) and
///   execute only the statements that create the outbox table and its indexes.</description></item>
/// </list>
/// <para>
/// A concurrent creation race from another instance is tolerated: if the table becomes present the
/// attempt is treated as success (Req 4.5). The whole attempt is bounded to 30 seconds; on failure
/// or timeout it logs and leaves <see cref="TableAvailable"/> as <c>false</c> so application startup
/// can continue (Req 4.3) and the store rejects operations (Req 4.4). Never throws (except caller
/// cancellation).
/// </para>
/// </remarks>
/// <typeparam name="TDbContext">The consumer-supplied EF Core context used to reach the database.</typeparam>
public sealed class SqlOutboxTableInitializer<TDbContext>
    where TDbContext : DbContext
{
    /// <summary>Upper bound for the whole table-creation attempt (Req 4.1, 4.3).</summary>
    private static readonly TimeSpan CreationTimeout = TimeSpan.FromSeconds(30);

    private readonly IDbContextFactory<TDbContext> _contextFactory;
    private readonly string _tableName;
    private readonly ILogger<SqlOutboxTableInitializer<TDbContext>> _logger;

    /// <summary>
    /// Creates the initializer.
    /// </summary>
    /// <param name="contextFactory">Factory used to obtain a dedicated context for the DDL command.</param>
    /// <param name="tableName">
    /// Outbox table name. When null or whitespace, <see cref="OutboxEntryConfiguration.DefaultTableName"/>
    /// is used. Must match the table name mapped for <see cref="OutboxEntry"/> on
    /// <typeparamref name="TDbContext"/>.
    /// </param>
    /// <param name="logger">Logger used to report a creation failure (Req 4.3).</param>
    public SqlOutboxTableInitializer(
        IDbContextFactory<TDbContext> contextFactory,
        string? tableName,
        ILogger<SqlOutboxTableInitializer<TDbContext>> logger)
    {
        _contextFactory = contextFactory ?? throw new ArgumentNullException(nameof(contextFactory));
        _tableName = string.IsNullOrWhiteSpace(tableName)
            ? OutboxEntryConfiguration.DefaultTableName
            : tableName;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// <c>true</c> once the outbox table is confirmed present (freshly created or already existing);
    /// <c>false</c> before initialization and after a failed/timed-out attempt.
    /// </summary>
    public bool TableAvailable { get; private set; }

    /// <summary>
    /// Ensures the outbox table exists, bounded to 30 seconds. Creates only the outbox table (safe on
    /// a shared business context). Idempotent and tolerant of concurrent creation. Never throws: on
    /// any failure or timeout it logs and leaves <see cref="TableAvailable"/> as <c>false</c> so
    /// startup continues (Req 4.3).
    /// </summary>
    /// <param name="ct">Caller cancellation token; linked with the 30s creation bound.</param>
    public async Task InitializeAsync(CancellationToken ct)
    {
        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutSource.CancelAfter(CreationTimeout);
        var linkedToken = timeoutSource.Token;

        try
        {
            await using var context = await _contextFactory
                .CreateDbContextAsync(linkedToken)
                .ConfigureAwait(false);

            context.Database.SetCommandTimeout(CreationTimeout);

            // 1) Already present? Nothing to do (Req 4.2).
            if (await TableExistsAsync(context, linkedToken).ConfigureAwait(false))
            {
                TableAvailable = true;
                return;
            }

            // 2) Create ONLY the outbox table using provider-correct DDL. EF's create script covers
            // the whole model; we execute only the statements for the outbox table so a shared
            // business context's other tables (and its migrations) are never touched.
            var statements = ExtractOutboxStatements(context);
            if (statements.Count == 0)
            {
                // Fallback: no statements matched (unusual). Treat as failure so startup continues.
                TableAvailable = false;
                _logger.LogError(
                    "Could not derive a create script for outbox table '{TableName}'; the outbox store will be unavailable and startup will continue.",
                    _tableName);
                return;
            }

            foreach (var statement in statements)
            {
                await context.Database.ExecuteSqlRawAsync(statement, linkedToken).ConfigureAwait(false);
            }

            TableAvailable = true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Caller-requested cancellation: propagate rather than swallow.
            TableAvailable = false;
            throw;
        }
        catch (OperationCanceledException)
        {
            // The 30s bound elapsed (Req 4.1, 4.3): log and allow startup to continue.
            TableAvailable = false;
            _logger.LogError(
                "Outbox table creation for '{TableName}' timed out after {TimeoutSeconds}s; the outbox store will be unavailable and startup will continue.",
                _tableName,
                CreationTimeout.TotalSeconds);
        }
        catch (Exception ex)
        {
            // A concurrent creator may have created the table between our check and create; if it is
            // now present, treat it as success (Req 4.5). Otherwise the store stays unavailable and
            // startup continues (Req 4.3).
            if (await TableExistsSafeAsync(ct).ConfigureAwait(false))
            {
                TableAvailable = true;
                return;
            }

            TableAvailable = false;
            _logger.LogError(
                ex,
                "Failed to create outbox table '{TableName}'; the outbox store will be unavailable and startup will continue.",
                _tableName);
        }
    }

    /// <summary>
    /// Extracts, from EF's full create script for the model, only the statements that create the
    /// outbox table and its indexes. Matching is done by the (unescaped) table name appearing in the
    /// statement, which is provider-agnostic enough for the CREATE TABLE / CREATE INDEX statements
    /// EF emits. Statements for any other (business) table are skipped.
    /// </summary>
    private List<string> ExtractOutboxStatements(TDbContext context)
    {
        var creator = context.Database.GetService<IRelationalDatabaseCreator>();
        var script = creator.GenerateCreateScript();

        var result = new List<string>();
        foreach (var raw in SplitStatements(script))
        {
            var statement = raw.Trim();
            if (statement.Length == 0)
            {
                continue;
            }

            // Keep only CREATE TABLE / CREATE INDEX (and ALTER TABLE for the same table, e.g. FKs or
            // constraints) statements that reference the outbox table name. This deliberately skips
            // statements for other tables in a shared business context.
            var mentionsOutboxTable = statement.Contains(_tableName, StringComparison.Ordinal);
            var isCreate = statement.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase)
                || statement.StartsWith("CREATE UNIQUE INDEX", StringComparison.OrdinalIgnoreCase)
                || statement.StartsWith("CREATE INDEX", StringComparison.OrdinalIgnoreCase)
                || statement.StartsWith("ALTER TABLE", StringComparison.OrdinalIgnoreCase);

            if (mentionsOutboxTable && isCreate)
            {
                result.Add(statement);
            }
        }

        return result;
    }

    /// <summary>
    /// Splits a create script into individual statements. EF separates statements with the
    /// statement terminator followed by a newline; we split on the common terminators used by the
    /// relational providers (";" and the SQL Server batch separator "GO").
    /// </summary>
    private static IEnumerable<string> SplitStatements(string script)
    {
        // Normalize line endings and split on ';'. This is sufficient for the CREATE TABLE / CREATE
        // INDEX statements EF generates for the outbox entity (no procedural bodies with inner ';').
        var normalized = script.Replace("\r\n", "\n", StringComparison.Ordinal);

        foreach (var part in normalized.Split(';'))
        {
            // Drop a lone "GO" batch separator line if present (SQL Server scripts).
            var cleaned = string.Join(
                '\n',
                part.Split('\n').Where(line => !line.Trim().Equals("GO", StringComparison.OrdinalIgnoreCase)));

            yield return cleaned;
        }
    }

    /// <summary>Checks whether the outbox table is queryable (i.e. exists) using the supplied context.</summary>
    private static async Task<bool> TableExistsAsync(TDbContext context, CancellationToken ct)
    {
        try
        {
            _ = await context.Set<OutboxEntry>()
                .AsNoTracking()
                .Take(1)
                .ToListAsync(ct)
                .ConfigureAwait(false);

            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Provider-agnostic existence check on a fresh context, used to tolerate a concurrent-creation
    /// race. Returns false on any error.
    /// </summary>
    private async Task<bool> TableExistsSafeAsync(CancellationToken ct)
    {
        try
        {
            await using var context = await _contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            context.Database.SetCommandTimeout(CreationTimeout);
            return await TableExistsAsync(context, ct).ConfigureAwait(false);
        }
        catch
        {
            return false;
        }
    }
}