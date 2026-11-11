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
import org.junit.jupiter.api.Test;

/** At-least-once delivery + idempotent consumer: the same payment may arrive twice; the balance changes once. */
class IdempotencyTest {

    static final PaymentCaptured P1 = pay("acc-1", 1, 100, "p1");

    static void drained(Lab lab) {
        await().atMost(Duration.ofSeconds(20)).until(() -> lab.lag() == 0);
    }

    @Test
    void producerRetrySameEventPublishedTwiceIsAppliedOnce() {
        try (var lab = new Lab()) {
            lab.startConsumer();
            lab.producer.publish(lab.topic, List.of(P1));
            lab.producer.publish(lab.topic, List.of(P1)); // app-level resend (e.g. timeout on the first attempt)
            drained(lab);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(100);
        }
    }

    @Test
    void failureAfterDbWriteBeforeOffsetCommitRedeliveryIsAppliedOnce() {
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
        }
    }
}
