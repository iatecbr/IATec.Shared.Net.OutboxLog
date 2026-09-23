using IATec.Shared.Net.OutboxLog.Configuration;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// Applies the global configuration defaults (<see cref="LogsOutboxOptions"/>) to a
/// <see cref="LogPayload"/> written directly to an <see cref="IOutboxStore"/> (i.e. not built by the
/// logging pipeline / LogPayloadFactory). Ensures that <c>containerKey</c> and <c>userId</c> are
/// taken from the global configuration when the caller did not supply them.
/// </summary>
internal static class OutboxPayloadDefaults
{
    /// <summary>
    /// Returns a payload whose <c>ContainerKey</c> and <c>UserId</c> are filled from the global
    /// configuration when they are empty on the supplied payload. All other fields are preserved.
    /// If nothing needs to change (or no options are available), the original payload is returned.
    /// </summary>
    /// <param name="payload">The caller-supplied payload.</param>
    /// <param name="options">The global options, or null when not configured.</param>
    /// <param name="serviceProvider">
    /// Service provider used to invoke <see cref="LogsOutboxOptions.UserIdProvider"/>, or null.
    /// </param>
    public static LogPayload Apply(
        LogPayload payload,
        LogsOutboxOptions? options,
        IServiceProvider? serviceProvider)
    {
        if (options is null)
        {
            return payload;
        }

        // containerKey: fall back to the globally configured ContainerKey.
        var containerKey = payload.ContainerKey;
        if (string.IsNullOrEmpty(containerKey) && !string.IsNullOrEmpty(options.ContainerKey))
        {
            containerKey = options.ContainerKey!;
        }

        // userId: fall back to the globally configured UserIdProvider.
        var userId = payload.UserId;
        if (string.IsNullOrEmpty(userId))
        {
            var resolved = ResolveUserId(options, serviceProvider);
            if (!string.IsNullOrEmpty(resolved))
            {
                userId = resolved!;
            }
        }

        // Nothing changed: keep the original instance.
        if (ReferenceEquals(containerKey, payload.ContainerKey) && ReferenceEquals(userId, payload.UserId))
        {
            return payload;
        }

        return payload with { ContainerKey = containerKey, UserId = userId };
    }

    /// <summary>
    /// Resolves the user id via <see cref="LogsOutboxOptions.UserIdProvider"/>. Never throws: any
    /// provider failure (or a missing provider / service provider) yields null.
    /// </summary>
    private static string? ResolveUserId(LogsOutboxOptions options, IServiceProvider? serviceProvider)
    {
        var provider = options.UserIdProvider;
        if (provider is null || serviceProvider is null)
        {
            return null;
        }

        try
        {
            return provider(serviceProvider);
        }
        catch
        {
            return null;
        }
    }
}