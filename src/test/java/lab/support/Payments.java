package lab.support;

import lab.payments.PaymentCaptured;

public final class Payments {
    private Payments() {}

    /** A EUR payment; paymentId defaults to {@code <account>-<sequence>}. */
    public static PaymentCaptured pay(String accountId, int sequence, long amountMinor) {
        return pay(accountId, sequence, amountMinor, accountId + "-" + sequence);
    }

    public static PaymentCaptured pay(String accountId, int sequence, long amountMinor, String paymentId) {
        return new PaymentCaptured(paymentId, accountId, amountMinor, "EUR", sequence);
    }
}
