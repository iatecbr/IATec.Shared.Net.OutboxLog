using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.Extensions.Logging;

namespace IATec.Shared.Net.OutboxLog.Logging;

/// <summary>
/// Builds a <see cref="LogPayload"/> from a log event, its category, active scopes and configured
/// context. Applies the field-mapping priority (innermost scope value → options → category /
/// EventId.Name → empty string), defaults missing required fields to the empty string, truncates the
/// content to 8192 characters, and never throws to the caller.
/// </summary>
public sealed class LogPayloadFactory
{
    /// <summary>Maximum number of characters retained in the payload content field.</summary>
    public const int MaxContentLength = 8192;

    private const string ContainerKeyName = "containerKey";
    private const string SourceKeyName = "source";
    private const string OwnerKeyName = "owner";
    private const string ActionKeyName = "action";
    private const string UserIdKeyName = "userId";

    private readonly IServiceProvider? _serviceProvider;

    /// <summary>
    /// Creates the factory. The optional <paramref name="serviceProvider"/> is passed to
    /// <see cref="LogsOutboxOptions.UserIdProvider"/> so a consumer can resolve the current user id
    /// (for example from an <c>IHttpContextAccessor</c>) as the fallback for the payload's
    /// <c>userId</c> field. When not supplied, the user-id fallback is skipped.
    /// </summary>
    /// <param name="serviceProvider">Application service provider used by the user-id fallback.</param>
    public LogPayloadFactory(IServiceProvider? serviceProvider = null)
        => _serviceProvider = serviceProvider;

    /// <summary>
    /// Builds a <see cref="LogPayload"/> from the log event, category, scopes and configured context.
    /// Missing required fields (containerKey, source, owner, action) become "". The content is
    /// truncated to <see cref="MaxContentLength"/> characters. This method never throws.
    /// </summary>
    public LogPayload Create<TState>(
        string category,
        LogLevel level,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter,
        IExternalScopeProvider? scopeProvider,
        LogsOutboxOptions options)
    {
        try
        {
            // Collect matching values from the scope stack, innermost scope winning. The scope
            // provider iterates scopes from outermost to innermost, so later matches overwrite
            // earlier ones and the innermost value ends up retained.
            string? scopeContainerKey = null;
            string? scopeSource = null;

            string? scopeOwner = null;
            string? scopeAction = null;
            string? scopeUserId = null;

            scopeProvider?.ForEachScope(
                (scope, _) => CollectScopeValues(
                    scope,
                    ref scopeContainerKey,
                    ref scopeSource,
                    ref scopeOwner,
                    ref scopeAction,
                    ref scopeUserId),
                (object?)null);

            var containerKey = FirstNonEmpty(scopeContainerKey, options?.ContainerKey);
            var source = FirstNonEmpty(scopeSource, options?.Source, category);

            // Fallbacks when no scope value is supplied (so the remote log bank, which requires
            // these fields, never receives an empty owner/action/userId):
            //   owner  : scope -> the logging category (the context class, e.g. the ILogger<T> type)
            //   action : scope -> EventId.Name -> the log level (Trace/Debug/Information/...)
            //   userId : scope -> options.UserIdProvider(serviceProvider) (e.g. the authenticated user)
            var owner = FirstNonEmpty(scopeOwner, category);
            var action = FirstNonEmpty(scopeAction, eventId.Name, level.ToString());
            var userId = FirstNonEmpty(scopeUserId, ResolveUserId(options));
            var content = Truncate(BuildContent(state, exception, formatter));

            return new LogPayload
            {
                ContainerKey = containerKey,
                Source = source,
                Owner = owner,
                Action = action,
                UserId = userId,
                Content = content,
            };
        }
        catch
        {
            // Never throw to the caller. Fall back to a payload with the fields we can salvage,
            // applying the same owner/action fallbacks as the happy path.
            return new LogPayload
            {
                ContainerKey = FirstNonEmpty(options?.ContainerKey),
                Source = FirstNonEmpty(options?.Source, category),
                Owner = FirstNonEmpty(category),
                Action = FirstNonEmpty(eventId.Name, level.ToString()),
            };
        }
    }

    /// <summary>
    /// Resolves the fallback user id via <see cref="LogsOutboxOptions.UserIdProvider"/> using the
    /// factory's service provider. Never throws: any provider failure (or a missing provider /
    /// service provider) is treated as "no user" and yields null so the caller falls back to "".
    /// </summary>
    private string? ResolveUserId(LogsOutboxOptions? options)
    {
        var provider = options?.UserIdProvider;
        if (provider is null || _serviceProvider is null)
        {
            return null;
        }

        try
        {
            return provider(_serviceProvider);
        }
        catch
        {
            return null;
        }
    }

    private static void CollectScopeValues(
        object? scope,
        ref string? containerKey,
        ref string? source,
        ref string? owner,
        ref string? action,
        ref string? userId)
    {
        if (scope is not IEnumerable<KeyValuePair<string, object>> pairs)
        {
            return;
        }

        foreach (var pair in pairs)
        {
            switch (pair.Key)
            {
                case ContainerKeyName:
                    AssignIfPresent(pair.Value, ref containerKey);
                    break;
                case SourceKeyName:
                    AssignIfPresent(pair.Value, ref source);
                    break;
                case OwnerKeyName:
                    AssignIfPresent(pair.Value, ref owner);
                    break;
                case ActionKeyName:
                    AssignIfPresent(pair.Value, ref action);
                    break;
                case UserIdKeyName:
                    AssignIfPresent(pair.Value, ref userId);
                    break;
            }
        }
    }

    private static void AssignIfPresent(object? value, ref string? target)
    {
        var text = value?.ToString();
        if (!string.IsNullOrEmpty(text))
        {
            target = text;
        }
    }

    private static string FirstNonEmpty(params string?[] candidates)
    {
        foreach (var candidate in candidates)
        {
            if (!string.IsNullOrEmpty(candidate))
            {
                return candidate!;
            }
        }

        return "";
    }

    private static string BuildContent<TState>(
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        string message;
        try
        {
            message = formatter is not null ? formatter(state, exception) ?? "" : "";
        }
        catch
        {
            message = "";
        }

        if (exception is null)
        {
            return message;
        }

        var detail = exception.ToString();
        return string.IsNullOrEmpty(message) ? detail : message + Environment.NewLine + detail;
    }

    private static string Truncate(string value)
    {
        if (string.IsNullOrEmpty(value) || value.Length <= MaxContentLength)
        {
            return value ?? "";
        }

        return value.Substring(0, MaxContentLength);
    }
}
