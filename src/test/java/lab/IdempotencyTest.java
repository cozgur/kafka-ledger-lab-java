package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lab.ledger.Faults;
import lab.payments.PaymentCaptured;
import lab.support.Lab;
import static lab.support.Lab.header;
import org.junit.jupiter.api.Test;

/** At-least-once delivery + idempotent consumer: the same payment may arrive twice; the balance changes once. */
class IdempotencyTest {

    static final PaymentCaptured P1 = pay("acc-1", 1, 100, "p1");

    static void drained(Lab lab) {
        await().atMost(Duration.ofSeconds(20)).until(() -> lab.lag() == 0);
    }

    @Test
    void applicationResendTheSameEventAsTwoRecordsIsAppliedOnceAndIsNotDeadLettered() throws Exception {
        try (var lab = new Lab()) {
            lab.startConsumer();
            lab.producer.publish(lab.topic, List.of(P1));
            lab.producer.publish(lab.topic, List.of(P1)); // the application sends it again: two separate Kafka records
            drained(lab);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(100);
            // Recognised as a duplicate, not mistaken for a conflict: a duplicate must not land in the DLT.
            assertThat(lab.ledger.appliedCount()).isEqualTo(1);
            assertThat(lab.dltCount()).isZero();
        }
    }

    @Test
    void failureAfterDbWriteBeforeOffsetCommitRedeliveryIsAppliedOnce() throws Exception {
        try (var lab = new Lab()) {
            var deliveries = new AtomicInteger();
            // The first delivery fails right after the DB transaction committed and before the offset is committed.
            // Spring's error handler seeks back and redelivers the same record; the ledger must recognise it.
            lab.startConsumer(new Faults() {
                @Override
                public void afterDbWrite(PaymentCaptured e, int partition, long offset) {
                    if (deliveries.incrementAndGet() == 1) throw new IllegalStateException("simulated crash after DB write");
                }
            });
            lab.producer.publish(lab.topic, List.of(P1));
            drained(lab);

            assertThat(deliveries.get()).as("the redelivery really happened").isEqualTo(2);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(100);
            assertThat(lab.ledger.appliedCount()).isEqualTo(1);
            assertThat(lab.dltCount()).isZero();
        }
    }

    @Test
    void aReusedPaymentIdWithDifferentContentIsAConflictDeadLetteredNeverApplied() throws Exception {
        // Same paymentId with a new sequence (e.g. an upstream retry that re-assigned the sequence). The sequence
        // check cannot catch it (2 is the next expected sequence); only the paymentId identity can.
        try (var lab = new Lab(1)) {
            lab.startConsumer();
            lab.producer.publish(lab.topic, List.of(P1, pay("acc-1", 2, 100, "p1")));
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 2, -30, "p2")));
            drained(lab);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(70);
            assertThat(lab.ledger.appliedSequences("acc-1")).containsExactly(1, 2);
            assertThat(header(lab.readDlt(1).get(0), "dlt.reason")).isEqualTo("payment-id-conflict");
        }
    }

    @Test
    void aParkedPaymentsIdReusedWithDifferentContentIsAConflictTooNotSilentlyDropped() throws Exception {
        try (var lab = new Lab(1)) {
            lab.startConsumer();
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 2, 10, "p2"), pay("acc-1", 2, 999, "p2"), pay("acc-1", 1, 100, "p1")));
            drained(lab);
            assertThat(lab.ledger.appliedSequences("acc-1")).containsExactly(1, 2);
            assertThat(lab.ledger.parkedSequences("acc-1")).isEmpty();
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(110); // the first p2 counts, the 999 never does
            var dead = lab.readDlt(1).get(0);
            assertThat(header(dead, "dlt.reason")).isEqualTo("payment-id-conflict");
            assertThat(dead.value()).contains("999");
        }
    }

    @Test
    void twoDifferentPaymentsClaimingTheSameParkedSequenceTheSecondIsASequenceConflict() throws Exception {
        try (var lab = new Lab(1)) {
            lab.startConsumer();
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 2, 10, "p2"), pay("acc-1", 2, 20, "p3"), pay("acc-1", 1, 100, "p1")));
            drained(lab);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(110);
            var dead = lab.readDlt(1).get(0);
            assertThat(header(dead, "dlt.reason")).isEqualTo("sequence-conflict");
            assertThat(dead.value()).contains("\"p3\"");
        }
    }
}
