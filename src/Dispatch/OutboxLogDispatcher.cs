using System.Reflection;
using System.Text.Json;

namespace IATec.Shared.Net.OutboxLog.Dispatch;

/// <summary>
/// Default <see cref="IOutboxLogDispatcher"/> implementation. Builds a <see cref="LogPayload"/> from the
/// supplied arguments and writes it to the injected <see cref="IOutboxStore"/>. The store applies the
/// global defaults for <c>containerKey</c> and <c>userId</c>, so this dispatcher only sets
/// <c>source</c>, <c>owner</c>, <c>action</c> and <c>content</c>.
/// </summary>
public sealed class OutboxLogDispatcher : IOutboxLogDispatcher
{
    /// <summary>Fallback source used when neither the caller nor the entry assembly provides one.</summary>
    private const string UnknownSource = "unknown";

    private static readonly string EntryAssemblySource =
        Assembly.GetEntryAssembly()?.GetName().Name ?? UnknownSource;

    private readonly IOutboxStore _outbox;

    /// <summary>
    /// Initializes a new instance backed by the supplied outbox store.
    /// </summary>
    /// <param name="outbox">The outbox store the built payload is written to.</param>
    /// <exception cref="ArgumentNullException"><paramref name="outbox"/> is null.</exception>
    public OutboxLogDispatcher(IOutboxStore outbox)
    {
        ArgumentNullException.ThrowIfNull(outbox);
        _outbox = outbox;
    }

    /// <inheritdoc />
    public Task<WriteResult> DispatchAsync(
        string? source,
        string owner,
        string action,
        object? content = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(owner);
        ArgumentException.ThrowIfNullOrWhiteSpace(action);

        var payload = new LogPayload
        {
            Source = string.IsNullOrWhiteSpace(source) ? EntryAssemblySource : source,
            Owner = owner,
            Action = action,
            Content = SerializeContent(content),
        };

        return _outbox.WriteAsync(payload, cancellationToken);
    }

    /// <summary>
    /// Converts <paramref name="content"/> to the string stored in the payload: null becomes an empty
    /// string, a <see cref="string"/> is returned verbatim, and any other object is serialized to JSON.
    /// </summary>
    private static string SerializeContent(object? content) => content switch
    {
        null => string.Empty,
        string s => s,
        _ => JsonSerializer.Serialize(content),
    };
}
