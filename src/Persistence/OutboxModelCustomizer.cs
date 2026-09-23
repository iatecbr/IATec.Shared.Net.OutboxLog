using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// EF Core <see cref="IModelCustomizer"/> that transparently adds the outbox entity
/// (<see cref="OutboxEntry"/>) to a consumer's <see cref="DbContext"/> model, so the consumer does
/// NOT need to call <c>modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration())</c> in its
/// own <c>OnModelCreating</c>.
/// </summary>
/// <remarks>
/// <para>
/// It derives from EF's default <see cref="ModelCustomizer"/> and calls <c>base.Customize</c> first,
/// so the consumer's own model (its business entities and <c>OnModelCreating</c>) is applied
/// exactly as before; then it applies <see cref="OutboxEntryConfiguration"/> on top. The outbox
/// entity is added idempotently (only if it is not already mapped, so an explicit
/// <c>ApplyConfiguration</c> by the consumer still works and does not conflict).
/// </para>
/// <para>
/// Because the outbox entity lives in the SAME context as the business entities, writes made while
/// a business transaction is open commit or roll back atomically with the business data
/// (transactional atomicity is preserved).
/// </para>
/// <para>
/// Enable it on the consumer's context options with
/// <c>.ReplaceService&lt;IModelCustomizer, OutboxModelCustomizer&gt;()</c> (done automatically by the
/// registration helper), without touching the consumer's <c>OnModelCreating</c>.
/// </para>
/// </remarks>
public sealed class OutboxModelCustomizer : ModelCustomizer
{
    /// <summary>Creates the customizer with EF's required dependencies.</summary>
    /// <param name="dependencies">EF-provided customizer dependencies.</param>
    public OutboxModelCustomizer(ModelCustomizerDependencies dependencies)
        : base(dependencies)
    {
    }

    /// <inheritdoc />
    public override void Customize(ModelBuilder modelBuilder, DbContext context)
    {
        // Apply the consumer's own model first (its business entities / OnModelCreating).
        base.Customize(modelBuilder, context);

        // Then add the outbox entity, unless the consumer already mapped it explicitly.
        if (modelBuilder.Model.FindEntityType(typeof(OutboxEntry)) is null)
        {
            modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration());
        }
    }
}