package lab.payments;

/**
 * The event contract between the payments producer and the ledger consumer.
 *
 * @param paymentId   globally unique; the idempotency key
 * @param accountId   the Kafka message key, so all events of one account land on one partition
 * @param amountMinor integer minor units (cents); never floating point for money
 * @param currency    ISO 4217
 * @param sequence    per-account, starts at 1, increments by 1
 */
public record PaymentCaptured(String paymentId, String accountId, long amountMinor, String currency, int sequence) {

    public static final String TOPIC = "payments.captured";
}
