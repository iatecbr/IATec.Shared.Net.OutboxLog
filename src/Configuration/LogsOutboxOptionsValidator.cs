namespace IATec.Shared.Net.OutboxLog.Configuration;

/// <summary>
/// Validates <see cref="LogsOutboxOptions"/> at registration time. Every check that fails
/// raises a <see cref="LogsOutboxConfigurationException"/> carrying the name of the offending
/// field so that registration can abort before mutating the DI container.
/// </summary>
public static class LogsOutboxOptionsValidator
{
    private const int RetryLimitMin = 1;
    private const int RetryLimitMax = 10;
    private const int BatchSizeMin = 1;
    private const int BatchSizeMax = 1000;

    private static readonly TimeSpan PollIntervalMin = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan PollIntervalMax = TimeSpan.FromSeconds(300);
    private static readonly TimeSpan RequestTimeoutMin = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan RequestTimeoutMax = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan RetryBackoffMin = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan RetryBackoffMax = TimeSpan.FromSeconds(3600);

    private const int CircuitBreakerFailuresMin = 1;
    private const int CircuitBreakerFailuresMax = 100;
    private static readonly TimeSpan CircuitBreakerDurationMin = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan CircuitBreakerDurationMax = TimeSpan.FromSeconds(3600);

    private const int HttpRetryCountMin = 0;
    private const int HttpRetryCountMax = 10;
    private static readonly TimeSpan HttpRetryDelayMin = TimeSpan.Zero;
    private static readonly TimeSpan HttpRetryDelayMax = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Validates the supplied options. Returns normally when every field is valid; otherwise
    /// throws <see cref="LogsOutboxConfigurationException"/> for the first offending field.
    /// </summary>
    /// <param name="options">The options instance to validate.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <c>null</c>.</exception>
    /// <exception cref="LogsOutboxConfigurationException">A field fails validation.</exception>
    public static void Validate(LogsOutboxOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.RetryLimit < RetryLimitMin || options.RetryLimit > RetryLimitMax)
        {
            throw new LogsOutboxConfigurationException(
                nameof(LogsOutboxOptions.RetryLimit),
                $"RetryLimit must be between {RetryLimitMin} and {RetryLimitMax} inclusive, but was {options.RetryLimit}.");
        }

        if (!Enum.IsDefined(typeof(OutboxStoreType), options.StoreType))
        {
            throw new LogsOutboxConfigurationException(
                nameof(LogsOutboxOptions.StoreType),
                $"StoreType '{options.StoreType}' is not a supported outbox store type.");
        }

        if (!IsWellFormedHttpUrl(options.LogBankEndpoint))
        {
            throw new LogsOutboxConfigurationException(
                nameof(LogsOutboxOptions.LogBankEndpoint),
                $"LogBankEndpoint must be a well-formed absolute http or https URL, but was '{options.LogBankEndpoint}'.");
        }

        if (options.PollInterval < PollIntervalMin || options.PollInterval > PollIntervalMax)
        {
            throw new LogsOutboxConfigurationException(
                nameof(LogsOutboxOptions.PollInterval),
                $"PollInterval must be between {PollIntervalMin.TotalSeconds}s and {PollIntervalMax.TotalSeconds}s inclusive, but was {options.PollInterval}.");
        }

        if (options.BatchSize < BatchSizeMin || options.BatchSize > BatchSizeMax)
        {
            throw new LogsOutboxConfigurationException(
                nameof(LogsOutboxOptions.BatchSize),
                $"BatchSize must be between {BatchSizeMin} and {BatchSizeMax} inclusive, but was {options.BatchSize}.");
        }

        if (options.RequestTimeout < RequestTimeoutMin || options.RequestTimeout > RequestTimeoutMax)
        {
            throw new LogsOutboxConfigurationException(
                nameof(LogsOutboxOptions.RequestTimeout),
                $"RequestTimeout must be between {RequestTimeoutMin.TotalSeconds}s and {RequestTimeoutMax.TotalSeconds}s inclusive, but was {options.RequestTimeout}.");
        }

        if (options.RetryBackoff < RetryBackoffMin || options.RetryBackoff > RetryBackoffMax)
        {
            throw new LogsOutboxConfigurationException(
                nameof(LogsOutboxOptions.RetryBackoff),
                $"RetryBackoff must be between {RetryBackoffMin.TotalSeconds}s and {RetryBackoffMax.TotalSeconds}s inclusive, but was {options.RetryBackoff}.");
        }

        // Circuit breaker: only meaningful when enabled, so its bounds are validated only then.
        if (options.UseCircuitBreaker)
        {
            if (options.CircuitBreakerFailuresAllowedBeforeBreaking < CircuitBreakerFailuresMin
                || options.CircuitBreakerFailuresAllowedBeforeBreaking > CircuitBreakerFailuresMax)
            {
                throw new LogsOutboxConfigurationException(
                    nameof(LogsOutboxOptions.CircuitBreakerFailuresAllowedBeforeBreaking),
                    $"CircuitBreakerFailuresAllowedBeforeBreaking must be between {CircuitBreakerFailuresMin} and {CircuitBreakerFailuresMax} inclusive, but was {options.CircuitBreakerFailuresAllowedBeforeBreaking}.");
            }

            if (options.CircuitBreakerDuration < CircuitBreakerDurationMin
                || options.CircuitBreakerDuration > CircuitBreakerDurationMax)
            {
                throw new LogsOutboxConfigurationException(
                    nameof(LogsOutboxOptions.CircuitBreakerDuration),
                    $"CircuitBreakerDuration must be between {CircuitBreakerDurationMin.TotalSeconds}s and {CircuitBreakerDurationMax.TotalSeconds}s inclusive, but was {options.CircuitBreakerDuration}.");
            }
        }

        // HTTP retry: only meaningful when enabled, so its bounds are validated only then.
        if (options.UseHttpRetry)
        {
            if (options.HttpRetryCount < HttpRetryCountMin || options.HttpRetryCount > HttpRetryCountMax)
            {
                throw new LogsOutboxConfigurationException(
                    nameof(LogsOutboxOptions.HttpRetryCount),
                    $"HttpRetryCount must be between {HttpRetryCountMin} and {HttpRetryCountMax} inclusive, but was {options.HttpRetryCount}.");
            }

            if (options.HttpRetryDelay < HttpRetryDelayMin || options.HttpRetryDelay > HttpRetryDelayMax)
            {
                throw new LogsOutboxConfigurationException(
                    nameof(LogsOutboxOptions.HttpRetryDelay),
                    $"HttpRetryDelay must be between {HttpRetryDelayMin.TotalSeconds}s and {HttpRetryDelayMax.TotalSeconds}s inclusive, but was {options.HttpRetryDelay}.");
            }
        }
    }

    private static bool IsWellFormedHttpUrl(string? candidate)
    {
        if (string.IsNullOrWhiteSpace(candidate))
        {
            return false;
        }

        return Uri.TryCreate(candidate, UriKind.Absolute, out var uri)
            && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps);
    }
}
