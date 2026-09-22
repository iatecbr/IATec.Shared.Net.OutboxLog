using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.HttpClient.Dto;
using IATec.Shared.HttpClient.Extensions;
using IATec.Shared.HttpClient.Service;
using Polly.CircuitBreaker;

namespace IATec.Shared.Net.OutboxLog.Dispatch;

/// <summary>
/// Delivers a <see cref="LogPayload"/> to the configured Log Bank Endpoint using the resilient
/// <see cref="IServiceClient"/> from IATec.Shared.HttpClient (Polly-based retry, circuit breaker and
/// timeout). The result is mapped to a <see cref="DeliveryResult"/>; expected delivery failures are
/// never thrown to the caller.
/// </summary>
/// <remarks>
/// When the circuit breaker is open (the endpoint has been failing), the underlying client short
/// circuits and raises a broken-circuit exception; that is mapped to
/// <see cref="DeliveryOutcome.Unreachable"/> so the dispatch worker leaves the entry Pending and
/// retries later with its own backoff, instead of hammering an unavailable endpoint.
/// </remarks>
public sealed class LogBankClient : ILogBankClient
{
    /// <summary>
    /// Named client registered via <c>AddHttpClientService(..., clientName: LogBankClientName)</c>
    /// and resolved through <see cref="IServiceClientFactory.Create"/>.
    /// </summary>
    public const string LogBankClientName = "IATec.Shared.Net.OutboxLog.LogBank";

    private readonly IServiceClient _client;
    private readonly string _path;

    /// <summary>
    /// Initializes the client. The <paramref name="serviceClientFactory"/> yields the named,
    /// policy-wrapped <see cref="IServiceClient"/>; the request path is derived from
    /// <see cref="LogsOutboxOptions.LogBankEndpoint"/> (its absolute path and query), since the base
    /// address is configured on the named client at registration time.
    /// </summary>
    public LogBankClient(IServiceClientFactory serviceClientFactory, LogsOutboxOptions options)
    {
        ArgumentNullException.ThrowIfNull(serviceClientFactory);
        ArgumentNullException.ThrowIfNull(options);

        _client = serviceClientFactory.Create(LogBankClientName);

        // The named client is configured with the endpoint's scheme/host/port as BaseAddress, so
        // here we POST to the endpoint's path (+ query). Using PathAndQuery keeps the request
        // relative to that base address.
        var endpoint = new Uri(options.LogBankEndpoint, UriKind.Absolute);
        _path = endpoint.PathAndQuery;
    }

    /// <inheritdoc />
    public async Task<DeliveryResult> SendAsync(LogPayload payload, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(payload);

        // Honor caller-initiated cancellation (e.g. worker shutdown) up front.
        ct.ThrowIfCancellationRequested();

        try
        {
            using var content = payload.GenerateStringContent("application/json");

            // The resilient client applies retry/circuit-breaker/timeout policies internally.
            ResponseDto<object> response = await _client
                .PostAsync<object>(_path, content)
                .ConfigureAwait(false);

            if (response.Success)
            {
                return new DeliveryResult(DeliveryOutcome.Accepted, FirstStatusCode(response));
            }

            // The endpoint responded but did not accept the payload.
            return new DeliveryResult(DeliveryOutcome.Rejected, FirstStatusCode(response));
        }
        catch (BrokenCircuitException)
        {
            // Circuit breaker is open: the endpoint is considered unavailable. Do not hammer it;
            // report Unreachable so the worker retries later with backoff.
            return new DeliveryResult(DeliveryOutcome.Unreachable, null);
        }
        catch (TimeoutException)
        {
            // Polly timeout strategy elapsed.
            return new DeliveryResult(DeliveryOutcome.TimedOut, null);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Caller-initiated cancellation propagates to the worker.
            throw;
        }
        catch (OperationCanceledException)
        {
            // Cancellation not initiated by the caller: treat as a timeout of the attempt.
            return new DeliveryResult(DeliveryOutcome.TimedOut, null);
        }
        catch (HttpRequestException)
        {
            // Connection refused, DNS/TLS failure, or other transport-level error.
            return new DeliveryResult(DeliveryOutcome.Unreachable, null);
        }
    }

    /// <summary>Extracts the first available HTTP status code from the response errors, if any.</summary>
    private static int? FirstStatusCode(BaseResponseDto response)
    {
        if (response.Errors is null)
        {
            return null;
        }

        foreach (var error in response.Errors)
        {
            if (error.StatusCode.HasValue)
            {
                return (int)error.StatusCode.Value;
            }
        }

        return null;
    }
}