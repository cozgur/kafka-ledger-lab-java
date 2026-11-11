package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import lab.payments.PaymentCaptured;
import lab.support.Lab;
import org.junit.jupiter.api.Test;

/**
 * Events of the same account (same key) are applied in order. Kafka only orders within a partition; keying by
 * accountId puts each account on one partition. Events of DIFFERENT accounts are not ordered relative to each other.
 */
class OrderingTest {

    @Test
    void perAccountOrderIsPreservedWhileAccountsInterleaveAcrossPartitions() throws Exception {
        try (var lab = new Lab(3)) {
            lab.startConsumer();
            var accounts = List.of("acc-a", "acc-b", "acc-c", "acc-d", "acc-e");
            List<PaymentCaptured> events = new ArrayList<>();
            for (int i = 1; i <= 20; i++) for (String a : accounts) events.add(pay(a, i, i)); // round-robin interleave
            lab.producer.publish(lab.topic, events);
            await().atMost(Duration.ofSeconds(20)).until(() -> lab.lag() == 0);

            // Precondition: traffic really spread over more than one partition (otherwise ordering is trivial).
            assertThat(lab.nonEmptyPartitions()).isGreaterThan(1);

            // The producer contract directly: every record's Kafka key is the accountId inside its payload.
            var json = new ObjectMapper();
            for (var r : lab.readAll()) assertThat(r.key()).isEqualTo(json.readTree(r.value()).get("accountId").asText());

            var expected = IntStream.rangeClosed(1, 20).boxed().toList();
            for (String a : accounts) {
                assertThat(lab.ledger.appliedSequences(a)).as("apply order for %s", a).isEqualTo(expected);
                // Kafka delivered them in order: nothing had to be parked. Keeps this test about the KEY choice,
                // so the ledger's own reordering cannot mask a wrong key.
                assertThat(lab.ledger.sequencesAppliedFromPark(a)).as("parked events for %s", a).isEmpty();
                assertThat(lab.ledger.balanceOf(a)).isEqualTo(210);
            }
        }
    }
}
