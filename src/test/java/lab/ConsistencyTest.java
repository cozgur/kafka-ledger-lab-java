package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lab.payments.PaymentCaptured;
import lab.support.Lab;
import org.junit.jupiter.api.Test;

/**
 * The ledger is consistent with the event stream. An independent auditor re-reads the topic and derives the expected
 * balances WITHOUT the ledger's code, then reconciles: per account stream balance == ledger balance; ledger balance
 * == sum of its applied payments; applied rows == unique valid payments; DLT messages == invalid payloads.
 */
class ConsistencyTest {

    @Test
    void reconcileTheEventStreamAgainstTheLedgerAfterAMessyStream() throws Exception {
        try (var lab = new Lab(3)) {
            lab.startConsumer();
            List<PaymentCaptured> events = new ArrayList<>();
            for (int ai = 0; ai < 20; ai++) {
                String a = String.format("acc-%02d", ai);
                for (int i = 0; i < 30; i++) events.add(pay(a, i + 1, ((ai + 1) * (i + 3)) % 97 - 40));
            }
            for (int i = 10; i < events.size() - 1; i += 37) Collections.swap(events, i, i + 1); // out-of-order
            List<PaymentCaptured> duplicates = new ArrayList<>();
            for (int i = 0; i < events.size(); i += 25) duplicates.add(events.get(i));
            String poison = "{\"paymentId\":\"bad\",\"accountId\":\"acc-00\",\"amountMinor\":1.5,\"currency\":\"EUR\",\"sequence\":31}";

            lab.producer.publish(lab.topic, events);
            lab.producer.publish(lab.topic, duplicates);
            lab.publishRaw(List.<String[]>of(new String[] {"acc-00", poison}));
            await().atMost(Duration.ofSeconds(30)).until(() -> lab.lag() == 0);

            // ---- Auditor: expectations from the topic only, never from the ledger's code ----
            var records = lab.readAll();
            assertThat(records).hasSize(events.size() + duplicates.size() + 1);
            var json = new ObjectMapper();
            Map<String, JsonNode> unique = new LinkedHashMap<>();
            int invalid = 0;
            for (var r : records) {
                JsonNode e = json.readTree(r.value());
                if (!e.get("amountMinor").isIntegralNumber()) { invalid++; continue; }
                unique.put(e.get("paymentId").asText(), e);
            }
            Map<String, Long> expected = new TreeMap<>();
            unique.values().forEach(e -> expected.merge(e.get("accountId").asText(), e.get("amountMinor").asLong(), Long::sum));

            assertThat(lab.ledger.balances()).isEqualTo(expected);          // (1) stream == ledger, per account
            assertThat(lab.ledger.internalMismatches()).isEmpty();          // (2) balance == sum of applied payments
            assertThat(lab.ledger.appliedCount()).isEqualTo(unique.size()); // (3) counts
            assertThat(lab.dltCount()).isEqualTo(invalid);
        }
    }
}
