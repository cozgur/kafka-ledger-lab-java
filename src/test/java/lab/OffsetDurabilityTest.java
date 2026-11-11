package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import lab.ledger.Faults;
import lab.payments.PaymentCaptured;
import lab.support.Lab;
import org.junit.jupiter.api.Test;

/**
 * The offset becomes durable on the broker only AFTER the DB transaction committed and the listener finished.
 * That is the loss window of at-least-once. (A real process kill is not covered.)
 */
class OffsetDurabilityTest {

    @Test
    void theOffsetIsNotCommittedBeforeTheDbWriteNorBeforeTheListenerCompletes() throws Exception {
        try (var lab = new Lab(1)) {
            var atBeforeDb = new CountDownLatch(1);
            var releaseBeforeDb = new CountDownLatch(1);
            var atAfterDb = new CountDownLatch(1);
            var releaseAfterDb = new CountDownLatch(1);
            lab.startConsumer(new Faults() {
                @Override
                public void beforeApply(PaymentCaptured e, int attempt) throws Exception {
                    atBeforeDb.countDown();
                    releaseBeforeDb.await();
                }

                @Override
                public void afterDbWrite(PaymentCaptured e, int partition, long offset) throws Exception {
                    atAfterDb.countDown();
                    releaseAfterDb.await();
                }
            });
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 1, 100)));
            try {
                // Phase 1: record received, DB not written. Over a 1s window the broker offset must not move.
                atBeforeDb.await();
                await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(() -> lab.committedOffset(0) <= 0);
                assertThat(lab.ledger.appliedCount()).isZero();

                // Phase 2: DB transaction committed, listener not finished. Still no durable offset.
                releaseBeforeDb.countDown();
                atAfterDb.await();
                assertThat(lab.ledger.appliedCount()).isEqualTo(1);
                await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(() -> lab.committedOffset(0) <= 0);

                // Phase 3: listener completes -> offset 1 becomes durable.
                releaseAfterDb.countDown();
                await().atMost(Duration.ofSeconds(10)).until(() -> lab.committedOffset(0) == 1);
                assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(100);
            } finally {
                releaseBeforeDb.countDown(); // never leave the listener blocked when an assertion fails
                releaseAfterDb.countDown();
            }
        }
    }
}
