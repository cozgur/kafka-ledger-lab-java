package lab;

import static lab.support.Lab.header;
import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import lab.support.Lab;
import org.junit.jupiter.api.Test;

/**
 * Out-of-order and late events follow the per-account sequence rule:
 * sequence == last + 1 -> apply, then drain parked successors; sequence > last + 1 -> park;
 * a NEW paymentId on a taken slot (applied or parked) -> sequence conflict -> DLT (never silently dropped).
 */
class OutOfOrderTest {

    static void drained(Lab lab) {
        await().atMost(Duration.ofSeconds(20)).until(() -> lab.lag() == 0);
    }

    @Test
    void arrival132IsAppliedAs123() {
        try (var lab = new Lab()) {
            lab.startConsumer();
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 1, 100), pay("acc-1", 3, 10), pay("acc-1", 2, -30)));
            drained(lab);
            assertThat(lab.ledger.appliedSequences("acc-1")).containsExactly(1, 2, 3);
            assertThat(lab.ledger.sequencesAppliedFromPark("acc-1")).containsExactly(3);
            assertThat(lab.ledger.parkedSequences("acc-1")).isEmpty();
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(80);
        }
    }

    @Test
    void arrival312IsAppliedAs123() {
        try (var lab = new Lab()) {
            lab.startConsumer();
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 3, 10), pay("acc-1", 1, 100), pay("acc-1", 2, -30)));
            drained(lab);
            assertThat(lab.ledger.appliedSequences("acc-1")).containsExactly(1, 2, 3);
            assertThat(lab.ledger.sequencesAppliedFromPark("acc-1")).containsExactly(3);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(80);
        }
    }

    @Test
    void aGapKeepsLaterEventsParkedUntilTheMissingEventArrives() {
        try (var lab = new Lab()) {
            lab.startConsumer();
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 1, 100), pay("acc-1", 3, 10)));
            drained(lab);
            assertThat(lab.ledger.appliedSequences("acc-1")).containsExactly(1);
            assertThat(lab.ledger.parkedSequences("acc-1")).containsExactly(3);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(100);

            lab.producer.publish(lab.topic, List.of(pay("acc-1", 2, -30))); // the late event arrives
            drained(lab);
            assertThat(lab.ledger.appliedSequences("acc-1")).containsExactly(1, 2, 3);
            assertThat(lab.ledger.parkedSequences("acc-1")).isEmpty();
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(80);
        }
    }

    @Test
    void anEventForAnAlreadyAppliedSequenceWithANewPaymentIdIsASequenceConflictNotSilentlyIgnored() throws Exception {
        try (var lab = new Lab()) {
            lab.startConsumer();
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 1, 100), pay("acc-1", 2, -30)));
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 1, 999, "p-late-replay")));
            drained(lab);
            assertThat(lab.ledger.appliedSequences("acc-1")).containsExactly(1, 2);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(70);
            assertThat(header(lab.readDlt(1).get(0), "dlt.reason")).isEqualTo("sequence-conflict");
        }
    }
}
