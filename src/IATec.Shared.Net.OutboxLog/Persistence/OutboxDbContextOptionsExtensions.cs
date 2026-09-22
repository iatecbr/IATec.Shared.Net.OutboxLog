using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// Extensions to enable the outbox entity on a consumer's <see cref="DbContext"/> without touching
/// its <c>OnModelCreating</c>.
/// </summary>
public static class OutboxDbContextOptionsExtensions
{
    /// <summary>
    /// Adds the outbox entity (<see cref="OutboxEntry"/>) to the context's model transparently, by
    /// replacing EF's model customizer with <see cref="OutboxModelCustomizer"/>. Call this when
    /// building the context options (for example inside <c>AddDbContextFactory</c> /
    /// <c>AddDbContext</c>), after the provider (<c>UseSqlServer</c>/<c>UseNpgsql</c>/...), instead
    /// of calling <c>modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration())</c> in
    /// <c>OnModelCreating</c>.
    /// </summary>
    /// <param name="optionsBuilder">The options builder being configured.</param>
    /// <returns>The same <paramref name="optionsBuilder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="optionsBuilder"/> is null.</exception>
    public static DbContextOptionsBuilder AddLogsOutboxModel(this DbContextOptionsBuilder optionsBuilder)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        optionsBuilder.ReplaceService<IModelCustomizer, OutboxModelCustomizer>();
        return optionsBuilder;
    }

    /// <summary>
    /// Strongly-typed overload of <see cref="AddLogsOutboxModel(DbContextOptionsBuilder)"/>.
    /// </summary>
    /// <typeparam name="TContext">The context type being configured.</typeparam>
    /// <param name="optionsBuilder">The options builder being configured.</param>
    /// <returns>The same <paramref name="optionsBuilder"/> for chaining.</returns>
    public static DbContextOptionsBuilder<TContext> AddLogsOutboxModel<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        optionsBuilder.ReplaceService<IModelCustomizer, OutboxModelCustomizer>();
        return optionsBuilder;
    }
}