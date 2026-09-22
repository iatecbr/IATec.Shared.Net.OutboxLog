namespace IATec.Shared.Net.OutboxLog.Dispatch;

/// <summary>
/// Sends a single <see cref="LogPayload"/> to the remote Log Bank Endpoint and reports the
/// classified outcome of the attempt. Implementations never throw for expected delivery
/// failures (rejection, timeout, unreachable endpoint); those are surfaced as
/// <see cref="DeliveryResult"/> values instead.
/// </summary>
public interface ILogBankClient
{
    /// <summary>
    /// POSTs the payload to the configured endpoint. Returns
    /// <see cref="DeliveryOutcome.Accepted"/>, <see cref="DeliveryOutcome.Rejected"/>,
    /// <see cref="DeliveryOutcome.TimedOut"/>, or <see cref="DeliveryOutcome.Unreachable"/>.
    /// </summary>
    /// <param name="payload">The log payload to deliver.</param>
    /// <param name="ct">
    /// A cancellation token linked to the caller's lifetime. Cancellation originating from this
    /// token propagates as an <see cref="OperationCanceledException"/>; cancellation caused by the
    /// per-request timeout is mapped to <see cref="DeliveryOutcome.TimedOut"/>.
    /// </param>
    Task<DeliveryResult> SendAsync(LogPayload payload, CancellationToken ct);
}
