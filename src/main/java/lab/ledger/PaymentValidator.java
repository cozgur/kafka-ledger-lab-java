package lab.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lab.payments.PaymentCaptured;

/** Payload validation. A payload that fails here can never succeed, so it is not worth retrying. */
public final class PaymentValidator {

    private static final ObjectMapper JSON = new ObjectMapper();

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
        JsonNode currency = o.get("currency");
        if (currency == null || !currency.isTextual() || !currency.asText().matches("[A-Z]{3}")) {
            throw new InvalidPayloadException("currency must be an ISO 4217 code");
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
