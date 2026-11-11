package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lab.ledger.Faults;
import lab.payments.PaymentCaptured;
import lab.support.Lab;
import org.junit.jupiter.api.Test;

/** A GRACEFUL stop mid-stream and a restart lose nothing and apply nothing twice. (A process kill is not tested.) */
class RestartTest {

    static final List<String> ACCOUNTS = List.of("acc-a", "acc-b", "acc-c", "acc-d", "acc-e");

    static List<PaymentCaptured> batch(int from, int to) {
        List<PaymentCaptured> events = new ArrayList<>();
        for (int i = from; i <= to; i++) for (String a : ACCOUNTS) events.add(pay(a, i, i));
        return events;
    }

    @Test
    void stopMidStreamKeepProducingRestartNoLossNoDoubleApply() throws Exception {
        try (var lab = new Lab(3)) {
            // Gate: from the 51st message on, every consumer thread pauses; we stop the container right there, so "mid-stream" is deterministic.
            var processed = new AtomicInteger();
            var atGate = new CountDownLatch(1);
            var gate = new CountDownLatch(1);
            var first = lab.startConsumer(new Faults() {
                @Override
                public void afterDbWrite(PaymentCaptured e, int partition, long offset) throws Exception {
                    if (processed.incrementAndGet() >= 51) { // every consumer thread stops here, not just one
                        atGate.countDown();
                        gate.await();
                    }
                }
            });
            lab.producer.publish(lab.topic, batch(1, 40)); // 200 events
            atGate.await();
            var stopping = CompletableFuture.runAsync(first::stop);
            gate.countDown();
            stopping.join();

            int appliedAtStop = lab.ledger.appliedCount();
            assertThat(appliedAtStop).isBetween(50, 199); // precondition: we really stopped in the middle
            assertThat(lab.lag()).isPositive();

            lab.producer.publish(lab.topic, batch(41, 50)); // traffic keeps arriving while the ledger is down
            var redelivered = new AtomicInteger();
            lab.startConsumer(new Faults() {
                @Override
                public void afterDbWrite(PaymentCaptured e, int partition, long offset) {
                    redelivered.incrementAndGet();
                }
            });
            await().atMost(Duration.ofSeconds(30)).until(() -> lab.lag() == 0);

            // Idempotency would hide a restart-from-zero in the balances, so check deliveries directly: only the
            // remaining events plus the uncommitted tail (at most one per partition here) may come again.
            int remaining = 250 - appliedAtStop;
            assertThat(redelivered.get()).isBetween(remaining, remaining + 3);
            assertThat(lab.ledger.appliedCount()).isEqualTo(250);
            var expected = IntStream.rangeClosed(1, 50).boxed().toList();
            for (String a : ACCOUNTS) {
                assertThat(lab.ledger.appliedSequences(a)).isEqualTo(expected);
                assertThat(lab.ledger.balanceOf(a)).isEqualTo(1275);
            }
        }
    }
}
