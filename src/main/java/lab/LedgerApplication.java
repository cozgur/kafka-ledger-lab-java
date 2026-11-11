package lab;

import lab.ledger.Faults;
import lab.ledger.LedgerConsumer;
import lab.ledger.LedgerRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/** The ledger as a Spring Boot service: repository + Spring Kafka listener container, configured from properties. */
@SpringBootApplication
public class LedgerApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgerApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    LedgerRepository ledgerRepository(
            @Value("${lab.db.url}") String url,
            @Value("${lab.db.user}") String user,
            @Value("${lab.db.password}") String password,
            @Value("${lab.db.schema:ledger}") String schema) {
        return new LedgerRepository(url, user, password, schema);
    }

    @Bean(destroyMethod = "close")
    LedgerConsumer ledgerConsumer(
            LedgerRepository ledger,
            @Value("${lab.kafka.bootstrap-servers}") String bootstrap,
            @Value("${lab.kafka.topic:payments.captured}") String topic,
            @Value("${lab.kafka.group-id:ledger}") String groupId,
            @Value("${lab.kafka.dlt.max-attempts:3}") int maxAttempts,
            @Value("${lab.kafka.dlt.backoff-ms:100}") long backoffMs) {
        return new LedgerConsumer(bootstrap, topic, groupId, ledger,
                new LedgerConsumer.DltPolicy(topic + ".dlt", maxAttempts, backoffMs), Faults.NONE);
    }
}
