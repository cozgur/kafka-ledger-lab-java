package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import lab.payments.PaymentsProducer;
import lab.support.Lab;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

/**
 * If the dead-letter topic cannot be written, the poison message must not be lost: its offset may not be committed until
 * the DLT write succeeds. The test makes this test's own DLT reject every record (topic-level max.message.bytes = 1),
 * checks that the source offset stays put and nothing reaches the DLT, then restores the DLT and expects the poison
 * message in the DLT and the payment behind it applied. Trade-off made explicit: while the DLT is down, the partition
 * waits behind the poison message (blocking is preferred to losing it).
 */
class DltWriteFailureTest {

    static void setDltMaxMessageBytes(Lab lab, String value) throws Exception {
        var dlt = new ConfigResource(ConfigResource.Type.TOPIC, lab.dltTopic);
        var op = value == null
                ? new AlterConfigOp(new ConfigEntry("max.message.bytes", ""), AlterConfigOp.OpType.DELETE)
                : new AlterConfigOp(new ConfigEntry("max.message.bytes", value), AlterConfigOp.OpType.SET);
        lab.admin.incrementalAlterConfigs(Map.of(dlt, List.of(op))).all().get();
    }

    @Test
    void aPoisonMessageIsNotLostWhileTheDltCannotBeWritten() throws Exception {
        try (var lab = new Lab(1)) {
            setDltMaxMessageBytes(lab, "1"); // every DLT write fails with RecordTooLargeException
            lab.startConsumer();
            String poison = "{\"paymentId\":\"bad-1\",\"accountId\":\"acc-1\",\"amountMinor\":\"abc\",\"currency\":\"EUR\",\"sequence\":1}";
            lab.publishRaw(List.of(
                    new String[] {"acc-1", poison},
                    new String[] {"acc-2", PaymentsProducer.toJson(pay("acc-2", 1, 50))}));

            // While the DLT rejects writes: the poison offset is never committed and nothing is (half-)written to the DLT.
            await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(6)).alias("source offset stays at the poison message")
                    .until(() -> lab.committedOffset(0) <= 0 && lab.dltCount() == 0);
            assertThat(lab.ledger.balanceOf("acc-2")).as("the payment behind the poison waits").isNull();

            setDltMaxMessageBytes(lab, null); // DLT writable again
            await().atMost(Duration.ofSeconds(30)).alias("lag 0 after the DLT recovered").until(() -> lab.lag() == 0);
            var dead = lab.readDlt(1).get(0);
            assertThat(dead.value()).isEqualTo(poison);
            assertThat(Lab.header(dead, "dlt.reason")).isEqualTo("invalid-payload");
            assertThat(lab.dltCount()).isEqualTo(1);
            assertThat(lab.ledger.balanceOf("acc-2")).isEqualTo(50);
        }
    }
}
