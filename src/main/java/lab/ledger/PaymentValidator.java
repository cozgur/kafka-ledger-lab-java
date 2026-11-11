package lab.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lab.payments.PaymentCaptured;

/** Payload validation. A payload that fails here can never succeed, so it is not worth retrying. */
public final class PaymentValidator {

    private static final ObjectMapper JSON = new ObjectMapper();
    /** The lab ledger is single-currency: balances are EUR minor units. Other currencies are rejected, never summed. */
    public static final String LEDGER_CURRENCY = "EUR";
    /** Per-event bound (10 billion EUR in cents); keeps sums far from bigint/long overflow. */
    public static final long MAX_ABS_AMOUNT_MINOR = 1_000_000_000_000L;

    private PaymentValidator() {}

    public static PaymentCaptured parse(String raw) {
        if (raw == null) throw new InvalidPayloadException("empty message value");
        JsonNode o;
        try {
            o = JSON.readTree(raw);
        } catch (Exception e) {
            throw new InvalidPayloadException("value is not valid JSON");
        }
        String paymentId = text(o, "paymentId");
        String accountId = text(o, "accountId");
        JsonNode amount = o.get("amountMinor");
        if (amount == null || !amount.canConvertToLong() || !amount.isIntegralNumber()) {
            throw new InvalidPayloadException("amountMinor must be an integer (minor units)");
        }
        // Direct bounds, not Math.abs: Math.abs(Long.MIN_VALUE) overflows to a negative number and would pass (review finding).
        if (amount.asLong() < -MAX_ABS_AMOUNT_MINOR || amount.asLong() > MAX_ABS_AMOUNT_MINOR) {
            throw new InvalidPayloadException("amountMinor must be within +/-" + MAX_ABS_AMOUNT_MINOR);
        }
        JsonNode currency = o.get("currency");
        if (currency == null || !currency.isTextual() || !LEDGER_CURRENCY.equals(currency.asText())) {
            throw new InvalidPayloadException("currency must be " + LEDGER_CURRENCY + " (single-currency ledger)");
        }
        JsonNode sequence = o.get("sequence");
        if (sequence == null || !sequence.isInt() || sequence.asInt() < 1) {
            throw new InvalidPayloadException("sequence must be a positive integer");
        }
        return new PaymentCaptured(paymentId, accountId, amount.asLong(), currency.asText(), sequence.asInt());
    }

    private static String text(JsonNode o, String field) {
        JsonNode v = o.get(field);
        if (v == null || !v.isTextual() || v.asText().isEmpty()) throw new InvalidPayloadException(field + " must be a non-empty string");
        return v.asText();
    }

    public static final class InvalidPayloadException extends RuntimeException {
        public InvalidPayloadException(String message) {
            super(message);
        }
    }
}
