namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// The delivery state of an <see cref="OutboxEntry"/>.
/// </summary>
public enum DeliveryStatus
{
    /// <summary>Awaiting delivery to the Log Bank Endpoint.</summary>
    Pending = 0,

    /// <summary>Successfully delivered to the Log Bank Endpoint.</summary>
    Delivered = 1,

    /// <summary>Reached the retry limit without successful delivery.</summary>
    Failed = 2
}
