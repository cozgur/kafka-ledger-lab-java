package lab.support;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
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
    public static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:8.1.0";
    @SuppressWarnings("resource")
    public static final GenericContainer<?> SCHEMA_REGISTRY = new GenericContainer<>(SCHEMA_REGISTRY_IMAGE)
            .withNetwork(NETWORK)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
            .withExposedPorts(8081)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
            .withStartupTimeout(STARTUP);

    static {
        // Only method references on the container OBJECTS here. A lambda written in this class compiles to a static
        // method of Infra; running it on another thread waits for Infra's class initialisation, which is itself waiting
        // in join() below -> class-init deadlock (hit twice: first with static fields, then with captured locals).
        CompletableFuture.allOf(
                CompletableFuture.runAsync(KAFKA::start).thenRun(SCHEMA_REGISTRY::start),
                CompletableFuture.runAsync(POSTGRES::start)).join();
    }

    private Infra() {}

    public static String schemaRegistryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
