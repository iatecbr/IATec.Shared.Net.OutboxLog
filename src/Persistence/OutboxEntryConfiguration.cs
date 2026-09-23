using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// EF Core entity configuration for <see cref="OutboxEntry"/>. Maps the entry to the
/// <c>LogsOutboxEntries</c> table (name overridable), enforces a unique deduplication key,
/// persists the <see cref="LogPayload"/> as a single JSON column, and adds a composite index
/// tuned for the pending-batch query.
/// </summary>
public sealed class OutboxEntryConfiguration : IEntityTypeConfiguration<OutboxEntry>
{
    /// <summary>The default table name used when none is supplied.</summary>
    public const string DefaultTableName = "LogsOutboxEntries";

    private static readonly JsonSerializerOptions PayloadSerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly string _tableName;

    /// <summary>
    /// Creates the configuration, optionally overriding the table name. When
    /// <paramref name="tableName"/> is null or whitespace, <see cref="DefaultTableName"/> is used.
    /// </summary>
    /// <param name="tableName">Optional table name override.</param>
    public OutboxEntryConfiguration(string? tableName = null)
    {
        _tableName = string.IsNullOrWhiteSpace(tableName) ? DefaultTableName : tableName;
    }

    /// <inheritdoc />
    public void Configure(EntityTypeBuilder<OutboxEntry> builder)
    {
        builder.ToTable(_tableName);

        // Client-generated GUID primary key (no store-generated value).
        builder.HasKey(e => e.Id);
        builder.Property(e => e.Id).ValueGeneratedNever();

        // SHA-256 hex is 64 chars. A bounded length keeps the unique index portable across
        // providers (e.g. MySQL requires a key length on indexed text columns).
        builder.Property(e => e.DeduplicationKey)
            .HasMaxLength(64)
            .IsRequired();

        // Enforces idempotency per deduplication key (Req 6.2).
        builder.HasIndex(e => e.DeduplicationKey)
            .IsUnique();

        // Serialize the payload to a single JSON text column via a value converter. No explicit
        // column type: EF maps an unbounded string to each provider's large-text type (nvarchar(max)
        // on SQL Server, text on PostgreSQL, longtext on MySQL, TEXT on SQLite), keeping this
        // provider-agnostic.
        var payloadConverter = new ValueConverter<LogPayload, string>(
            payload => JsonSerializer.Serialize(payload, PayloadSerializerOptions),
            json => Deserialize(json));

        builder.Property(e => e.Payload)
            .HasConversion(payloadConverter)
            .HasColumnName("PayloadJson")
            .IsRequired();

        builder.Property(e => e.Status).IsRequired();
        builder.Property(e => e.AttemptCount).IsRequired();
        builder.Property(e => e.CreatedAt).IsRequired();

        // Tunes GetPendingAsync: filter by Status, gate by NextAttemptAt, order by CreatedAt.
        builder.HasIndex(e => new { e.Status, e.NextAttemptAt, e.CreatedAt });
    }

    private static LogPayload Deserialize(string json)
        => JsonSerializer.Deserialize<LogPayload>(json, PayloadSerializerOptions) ?? new LogPayload();
}
