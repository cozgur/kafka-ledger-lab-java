package lab.ledger;

import lab.payments.PaymentCaptured;

/** Test-only fault injection points in the ledger listener. Production uses {@link #NONE}. */
public interface Faults {

    /** Before each processing attempt; throw to simulate a processing failure. */
    default void beforeApply(PaymentCaptured event, int attempt) throws Exception {}

    /**
     * After the DB transaction committed, before the listener returns (so before the offset is committed).
     * Throw to simulate a crash in exactly that window: the DB change is durable, the offset is not.
     */
    default void afterDbWrite(PaymentCaptured event, int partition, long offset) throws Exception {}

    Faults NONE = new Faults() {};
}
