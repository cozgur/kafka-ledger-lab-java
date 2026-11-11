package lab.support;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Real infrastructure, started ONCE per test JVM and shared by every test class.
 * Each test isolates itself with unique topic / consumer-group / DB-schema names (see {@link Lab}).
 */
public final class Infra {

    public static final String KAFKA_IMAGE = "apache/kafka:4.1.1"; // Apache Kafka, KRaft mode
    public static final String POSTGRES_IMAGE = "postgres:17-alpine";
    private static final Duration STARTUP = Duration.ofMinutes(2);

    public static final Network NETWORK = Network.newNetwork();
    public static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092") // reachable from other containers on the network (Schema Registry)
            .withStartupTimeout(STARTUP);
    public static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(POSTGRES_IMAGE).withStartupTimeout(STARTUP);

    static {
        CompletableFuture.allOf(CompletableFuture.runAsync(KAFKA::start), CompletableFuture.runAsync(POSTGRES::start)).join();
    }

    private Infra() {}

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
