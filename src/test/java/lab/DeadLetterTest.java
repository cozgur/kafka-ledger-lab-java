package lab;

import static lab.support.Lab.header;
import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lab.ledger.Faults;
import lab.payments.PaymentCaptured;
import lab.support.Lab;
import org.junit.jupiter.api.Test;

/**
 * A message that cannot be processed goes to a dead-letter topic (DLT) and does not block its partition:
 * invalid payload -> DLT at once; processing error -> retried up to 3 attempts, then DLT; transient error -> recovers.
 * One partition, so the poison message and the good ones share it.
 */
class DeadLetterTest {

    static void drained(Lab lab) {
        await().atMost(Duration.ofSeconds(20)).until(() -> lab.lag() == 0);
    }

    static String json(PaymentCaptured p) {
        return lab.payments.PaymentsProducer.toJson(p);
    }

    @Test
    void invalidPayloadGoesToTheDltImmediatelyAndLaterEventsStillFlow() throws Exception {
        try (var lab = new Lab(1)) {
            lab.startConsumer();
            String poison = "{\"paymentId\":\"bad-1\",\"accountId\":\"acc-2\",\"amountMinor\":\"abc\",\"currency\":\"EUR\",\"sequence\":1}";
            lab.publishRaw(List.of(
                    new String[] {"acc-1", json(pay("acc-1", 1, 100))},
                    new String[] {"acc-2", poison},
                    new String[] {"acc-3", json(pay("acc-3", 1, 50))})); // behind the poison message
            drained(lab);

            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(100);
            assertThat(lab.ledger.balanceOf("acc-3")).isEqualTo(50);
            assertThat(lab.ledger.balanceOf("acc-2")).isNull();

            var dead = lab.readDlt(1).get(0);
            assertThat(dead.key()).isEqualTo("acc-2");
            assertThat(dead.value()).isEqualTo(poison); // original bytes, so it can be fixed and replayed
            assertThat(header(dead, "dlt.original.topic")).isEqualTo(lab.topic);
            assertThat(header(dead, "dlt.original.partition")).isEqualTo("0");
            assertThat(header(dead, "dlt.original.offset")).isEqualTo("1");
            assertThat(header(dead, "dlt.reason")).isEqualTo("invalid-payload");
            assertThat(header(dead, "dlt.attempts")).isEqualTo("1");
            assertThat(header(dead, "dlt.error")).contains("amountMinor");
            assertThat(lab.dltCount()).isEqualTo(1);
        }
    }

    @Test
    void aProcessingErrorIsRetried3TimesThenDeadLetteredWithoutBlocking() {
        try (var lab = new Lab(1)) {
            var attempts = new CopyOnWriteArrayList<Integer>();
            lab.startConsumer(new Faults() {
                @Override
                public void beforeApply(PaymentCaptured e, int attempt) {
                    if (!e.paymentId().equals("acc-2-1")) return;
                    attempts.add(attempt);
                    throw new IllegalStateException("simulated DB outage");
                }
            });
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 1, 100), pay("acc-2", 1, 20), pay("acc-3", 1, 50)));
            drained(lab);

            assertThat(attempts).containsExactly(1, 2, 3);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(100);
            assertThat(lab.ledger.balanceOf("acc-3")).isEqualTo(50);
            var dead = lab.readDlt(1).get(0);
            assertThat(header(dead, "dlt.original.offset")).isEqualTo("1");
            assertThat(header(dead, "dlt.reason")).isEqualTo("processing-failed");
            assertThat(header(dead, "dlt.attempts")).isEqualTo("3");
            assertThat(header(dead, "dlt.error")).contains("simulated DB outage");
        }
    }

    @Test
    void aTransientErrorThatRecoversWithinTheRetriesIsAppliedAndNotDeadLettered() throws Exception {
        try (var lab = new Lab(1)) {
            var attempts = new CopyOnWriteArrayList<Integer>();
            lab.startConsumer(new Faults() {
                @Override
                public void beforeApply(PaymentCaptured e, int attempt) {
                    if (!e.paymentId().equals("acc-1-1")) return;
                    attempts.add(attempt);
                    if (attempt == 1) throw new IllegalStateException("simulated blip");
                }
            });
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 1, 100)));
            drained(lab);

            assertThat(attempts).as("the failure happened and the retry fixed it").containsExactly(1, 2);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(100);
            assertThat(lab.dltCount()).isZero();
        }
    }
}
