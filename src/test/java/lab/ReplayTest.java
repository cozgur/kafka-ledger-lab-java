package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import lab.payments.PaymentCaptured;
import lab.support.Lab;
import org.junit.jupiter.api.Test;

/**
 * The ledger can be rebuilt from the topic: wipe it, rewind the consumer group, replay -> same state.
 * Only as good as the retained history: the second test shows what happens when retention removed the start of it.
 */
class ReplayTest {

    static void drained(Lab lab) {
        await().atMost(Duration.ofSeconds(30)).until(() -> lab.lag() == 0);
    }

    @Test
    void wipeResetOffsetsReplayIdenticalFinalState() throws Exception {
        try (var lab = new Lab(3)) {
            var live = lab.startConsumer();
            var accounts = List.of("acc-a", "acc-b", "acc-c", "acc-d");
            List<PaymentCaptured> events = new ArrayList<>();
            for (int i = 0; i < 25; i++) for (String a : accounts) events.add(pay(a, i + 1, (i % 2 == 1 ? -1 : 3) * (i + 1)));
            Collections.swap(events, 4, 8); // messy history: an out-of-order pair...
            lab.producer.publish(lab.topic, events);
            lab.producer.publish(lab.topic, List.of(events.get(0))); // ...and a duplicate send
            drained(lab);
            var before = lab.ledger.snapshot();
            assertThat(before).hasSize(4);

            live.stop();
            lab.ledger.wipe();
            assertThat(lab.ledger.snapshot()).isEmpty();
            lab.resetGroupToEarliest();
            assertThat(lab.lag()).isEqualTo(events.size() + 1); // the whole topic is "unread" again

            lab.startConsumer();
            drained(lab);
            assertThat(lab.ledger.snapshot()).isEqualTo(before);
            for (String a : accounts) assertThat(lab.ledger.appliedSequences(a)).isEqualTo(IntStream.rangeClosed(1, 25).boxed().toList());
        }
    }

    @Test
    void replayAfterRetentionRemovedTheOldestEventsParksInsteadOfGuessing() throws Exception {
        try (var lab = new Lab(1)) {
            var live = lab.startConsumer();
            lab.producer.publish(lab.topic, IntStream.rangeClosed(1, 10).mapToObj(i -> pay("acc-1", i, 10)).toList());
            drained(lab);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(100);

            live.stop();
            lab.ledger.wipe();
            lab.deleteRecordsBefore(0, 3); // "retention" removed sequences 1..3
            lab.resetGroupToEarliest();
            lab.startConsumer();
            drained(lab);

            assertThat(lab.ledger.appliedSequences("acc-1")).isEmpty();
            assertThat(lab.ledger.parkedSequences("acc-1")).containsExactly(4, 5, 6, 7, 8, 9, 10);
            assertThat(lab.ledger.balanceOf("acc-1")).isZero();
        }
    }
}
