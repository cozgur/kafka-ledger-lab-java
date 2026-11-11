package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lab.ledger.Faults;
import lab.payments.PaymentCaptured;
import lab.support.Lab;
import org.junit.jupiter.api.Test;

/**
 * Rebalance between two consumer instances of the same group: a second instance joins while the first is consuming
 * (partitions are redistributed), then the first one leaves (the second takes over everything). No payment is lost,
 * none is applied twice, and per-account order holds across the hand-overs.
 *
 * 6 partitions because each instance runs 3 consumer threads: with 3 partitions the joining instance could end up
 * with no partition at all and the test would prove nothing. Graceful leave only; a killed instance is not covered.
 */
class RebalanceTest {

    static final List<String> ACCOUNTS = IntStream.range(0, 12).mapToObj(i -> "acc-" + i).toList();

    static List<PaymentCaptured> batch(int from, int to) {
        List<PaymentCaptured> events = new ArrayList<>();
        for (int i = from; i <= to; i++) for (String a : ACCOUNTS) events.add(pay(a, i, i));
        return events;
    }

    static Faults counting(AtomicInteger counter) {
        return new Faults() {
            @Override
            public void afterDbWrite(PaymentCaptured e, int partition, long offset) {
                counter.incrementAndGet();
            }
        };
    }

    @Test
    void aSecondInstanceJoinsThenTheFirstLeavesNoLossNoDoubleApplyOrderKept() {
        try (var lab = new Lab(6)) {
            var byFirst = new AtomicInteger();
            var bySecond = new AtomicInteger();

            var first = lab.startConsumer(counting(byFirst));
            lab.producer.publish(lab.topic, batch(1, 10)); // 120 events
            await().atMost(Duration.ofSeconds(20)).until(() -> byFirst.get() > 0);

            var second = lab.startConsumer(counting(bySecond)); // same group: partitions are redistributed
            // Precondition: the rebalance really happened (both instances own partitions) BEFORE more traffic is sent.
            // Without this, the first instance had already consumed everything by the time the second one was assigned
            // partitions, and the second never processed a record (seen 5/5 runs).
            await().atMost(Duration.ofSeconds(30)).alias("partitions split between both instances")
                    .until(() -> second.assignedPartitionCount() > 0 && first.assignedPartitionCount() > 0
                            && first.assignedPartitionCount() + second.assignedPartitionCount() == 6);
            lab.producer.publish(lab.topic, batch(11, 20)); // 120 more, spread over both instances
            await().atMost(Duration.ofSeconds(30)).alias("second instance processed records").until(() -> bySecond.get() > 0);

            first.stop(); // the first instance leaves: the second takes over all partitions
            lab.producer.publish(lab.topic, batch(21, 25)); // 60 more after the hand-over
            await().atMost(Duration.ofSeconds(30)).until(() -> lab.lag() == 0);

            int total = ACCOUNTS.size() * 25;
            assertThat(lab.ledger.appliedCount()).isEqualTo(total);
            var expected = IntStream.rangeClosed(1, 25).boxed().toList();
            for (String a : ACCOUNTS) {
                assertThat(lab.ledger.appliedSequences(a)).as("apply order for %s", a).isEqualTo(expected);
                assertThat(lab.ledger.balanceOf(a)).isEqualTo(325);
            }
            // Deliveries across both instances: everything once, plus at most a small uncommitted tail redelivered at a
            // hand-over (idempotency would hide a larger replay in the balances, so bound it directly).
            int deliveries = byFirst.get() + bySecond.get();
            assertThat(deliveries).isBetween(total, total + 12);
            assertThat(byFirst.get()).isPositive();
        }
    }
}
