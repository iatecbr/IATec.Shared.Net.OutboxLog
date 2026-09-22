using CsCheck;
using IATec.Shared.Net.OutboxLog.Configuration;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests for <see cref="LogsOutboxOptionsValidator"/>.
/// </summary>
public class LogsOutboxOptionsValidatorPropertyTests
{
    /// <summary>
    /// Builds an otherwise-valid options instance so that a single deliberately-invalid
    /// field is the only reason validation can fail. Callers mutate one field.
    /// </summary>
    private static LogsOutboxOptions ValidOptions() => new()
    {
        LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log",
        RetryLimit = 3,
        StoreType = OutboxStoreType.InMemory,
        PollInterval = TimeSpan.FromSeconds(5),
        BatchSize = 100,
        RequestTimeout = TimeSpan.FromSeconds(10),
        RetryBackoff = TimeSpan.FromSeconds(30),
    };

    // Feature: logs-outbox-library, Property 18: Invalid configuration is rejected and leaves the container unmodified
    // Validates: Requirements 1.6, 1.8
    //
    // NOTE: The "container-unmodified" half of Property 18 (asserting the DI service collection's
    // registration count is unchanged after a rejected config) is deferred to task 12.2, once the
    // AddLogsOutbox registration extension exists. This test covers the validation half: an invalid
    // field is rejected with an error identifying the offending field.
    [Fact]
    public void InvalidRetryLimit_IsRejected_IdentifyingRetryLimitField()
    {
        // Generate RetryLimit values outside the inclusive range [1, 10]: either below 1 or above 10.
        // Cover the immediate boundaries (0, 11) as well as extreme values (int.MinValue/MaxValue).
        Gen<int> outOfRange = Gen.OneOf(
            Gen.Int[int.MinValue, 0],
            Gen.Int[11, int.MaxValue]);

        outOfRange.Sample(retryLimit =>
        {
            var options = ValidOptions();
            options.RetryLimit = retryLimit;

            var ex = Assert.Throws<LogsOutboxConfigurationException>(
                () => LogsOutboxOptionsValidator.Validate(options));

            Assert.Equal(nameof(LogsOutboxOptions.RetryLimit), ex.FieldName);
        }, iter: 100);
    }

    // Feature: logs-outbox-library, Property 18: Invalid configuration is rejected and leaves the container unmodified
    // Validates: Requirements 1.6, 1.8
    //
    // NOTE: The "container-unmodified" half of Property 18 is deferred to task 12.2 (see above).
    [Fact]
    public void MalformedEndpoint_IsRejected_IdentifyingLogBankEndpointField()
    {
        // Generate endpoint strings that are NOT well-formed absolute http/https URLs:
        // empty/whitespace, relative paths, non-http schemes, and free-form garbage. A random
        // free-form string is extremely unlikely to parse as an absolute http/https URI, but we
        // filter to be certain we only feed genuinely malformed inputs to the assertion.
        Gen<string> malformed = Gen.OneOf(
            Gen.Const(""),
            Gen.Const("   "),
            Gen.Const("/relative/path"),
            Gen.Const("relative/path"),
            Gen.Const("api-is-logs-dev.sdasystems.org/v1/log"),
            Gen.Const("ftp://example.com/x"),
            Gen.Const("file:///etc/hosts"),
            Gen.Const("mailto:someone@example.com"),
            Gen.Const("http://"),
            Gen.Const("https://"),
            Gen.Const("not a url"),
            Gen.Const("http:// space in host"),
            Gen.String)
            .Where(s => !IsWellFormedHttpUrl(s));

        malformed.Sample(endpoint =>
        {
            var options = ValidOptions();
            options.LogBankEndpoint = endpoint;

            var ex = Assert.Throws<LogsOutboxConfigurationException>(
                () => LogsOutboxOptionsValidator.Validate(options));

            Assert.Equal(nameof(LogsOutboxOptions.LogBankEndpoint), ex.FieldName);
        }, iter: 100);
    }

    // Circuit breaker: fallos fuera de [1, 100] se rechazan identificando el campo (solo cuando esta habilitado).
    [Fact]
    public void InvalidCircuitBreakerFailures_IsRejected_IdentifyingField()
    {
        Gen<int> outOfRange = Gen.OneOf(Gen.Int[int.MinValue, 0], Gen.Int[101, int.MaxValue]);

        outOfRange.Sample(failures =>
        {
            var options = ValidOptions();
            options.UseCircuitBreaker = true;
            options.CircuitBreakerFailuresAllowedBeforeBreaking = failures;

            var ex = Assert.Throws<LogsOutboxConfigurationException>(
                () => LogsOutboxOptionsValidator.Validate(options));

            Assert.Equal(nameof(LogsOutboxOptions.CircuitBreakerFailuresAllowedBeforeBreaking), ex.FieldName);
        }, iter: 100);
    }

    // Circuit breaker: duracion fuera de [1s, 3600s] se rechaza identificando el campo.
    [Fact]
    public void InvalidCircuitBreakerDuration_IsRejected_IdentifyingField()
    {
        Gen<TimeSpan> outOfRange = Gen.OneOf(
            Gen.Int[-3600, 0].Select(s => TimeSpan.FromSeconds(s)),
            Gen.Int[3601, 100000].Select(s => TimeSpan.FromSeconds(s)));

        outOfRange.Sample(duration =>
        {
            var options = ValidOptions();
            options.UseCircuitBreaker = true;
            options.CircuitBreakerDuration = duration;

            var ex = Assert.Throws<LogsOutboxConfigurationException>(
                () => LogsOutboxOptionsValidator.Validate(options));

            Assert.Equal(nameof(LogsOutboxOptions.CircuitBreakerDuration), ex.FieldName);
        }, iter: 100);
    }

    // Circuit breaker deshabilitado: valores fuera de rango NO se validan (son irrelevantes).
    [Fact]
    public void CircuitBreakerDisabled_DoesNotValidateItsBounds()
    {
        var options = ValidOptions();
        options.UseCircuitBreaker = false;
        options.CircuitBreakerFailuresAllowedBeforeBreaking = 99999; // fuera de rango, pero irrelevante
        options.CircuitBreakerDuration = TimeSpan.FromDays(1);       // fuera de rango, pero irrelevante

        // No debe lanzar: el circuit breaker esta deshabilitado.
        LogsOutboxOptionsValidator.Validate(options);
    }

    // HTTP retry: count fuera de [0, 10] se rechaza identificando el campo (solo cuando esta habilitado).
    [Fact]
    public void InvalidHttpRetryCount_IsRejected_IdentifyingField()
    {
        Gen<int> outOfRange = Gen.OneOf(Gen.Int[int.MinValue, -1], Gen.Int[11, int.MaxValue]);

        outOfRange.Sample(count =>
        {
            var options = ValidOptions();
            options.UseHttpRetry = true;
            options.HttpRetryCount = count;
            options.HttpRetryDelay = TimeSpan.FromSeconds(1); // valido

            var ex = Assert.Throws<LogsOutboxConfigurationException>(
                () => LogsOutboxOptionsValidator.Validate(options));

            Assert.Equal(nameof(LogsOutboxOptions.HttpRetryCount), ex.FieldName);
        }, iter: 100);
    }

    // HTTP retry deshabilitado: valores fuera de rango NO se validan.
    [Fact]
    public void HttpRetryDisabled_DoesNotValidateItsBounds()
    {
        var options = ValidOptions();
        options.UseHttpRetry = false;
        options.HttpRetryCount = 99999;               // fuera de rango, pero irrelevante
        options.HttpRetryDelay = TimeSpan.FromHours(1); // fuera de rango, pero irrelevante

        LogsOutboxOptionsValidator.Validate(options);
    }

    /// <summary>
    /// Mirrors the validator's own well-formedness rule so the generator can exclude any
    /// randomly-generated string that happens to be a valid absolute http/https URL.
    /// </summary>
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
