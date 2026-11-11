package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lab.ledger.LedgerRepository;
import lab.payments.PaymentsProducer;
import lab.support.Infra;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** The Spring Boot wiring: the application context starts, consumes from its configured topic and updates the ledger. */
@SpringBootTest
class LedgerApplicationTest {

    static final String ID = UUID.randomUUID().toString().substring(0, 8);
    static final String TOPIC = "payments.captured.app-" + ID;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Infra.bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1), new NewTopic(TOPIC + ".dlt", 1, (short) 1))).all().get();
        }
        registry.add("lab.kafka.bootstrap-servers", Infra::bootstrapServers);
        registry.add("lab.kafka.topic", () -> TOPIC);
        registry.add("lab.kafka.group-id", () -> "ledger-app-" + ID);
        registry.add("lab.db.url", Infra.POSTGRES::getJdbcUrl);
        registry.add("lab.db.user", Infra.POSTGRES::getUsername);
        registry.add("lab.db.password", Infra.POSTGRES::getPassword);
        registry.add("lab.db.schema", () -> "app_" + ID);
    }

    @Autowired
    LedgerRepository ledger;

    @Test
    void theBootApplicationConsumesAndAppliesPayments() {
        try (var producer = new PaymentsProducer(Infra.bootstrapServers())) {
            producer.publish(TOPIC, List.of(pay("acc-1", 1, 100), pay("acc-1", 2, -30)));
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> Long.valueOf(70).equals(ledger.balanceOf("acc-1")));
        assertThat(ledger.appliedSequences("acc-1")).containsExactly(1, 2);
    }
}
