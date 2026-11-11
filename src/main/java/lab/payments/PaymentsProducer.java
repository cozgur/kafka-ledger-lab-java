package lab.payments;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/** Payments side: publishes PaymentCaptured events keyed by accountId. */
public class PaymentsProducer implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();
    private final DefaultKafkaProducerFactory<String, String> factory;
    private final KafkaTemplate<String, String> template;

    public PaymentsProducer(String bootstrapServers) {
        this(bootstrapServers, Map.of());
    }

    /** @param overrides extra producer settings (tests use this, e.g. a short request timeout to force retries) */
    public PaymentsProducer(String bootstrapServers, Map<String, Object> overrides) {
        var config = new HashMap<String, Object>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                // Idempotent producer: the BROKER drops duplicates caused by the producer's own retries.
                // It does NOT stop the application from sending the same event twice -> the consumer must be idempotent too.
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all"));
        config.putAll(overrides);
        this.factory = new DefaultKafkaProducerFactory<>(config);
        this.template = new KafkaTemplate<>(factory);
    }

    /** Sends all events and waits until the broker acknowledged every one of them. */
    public void publish(String topic, List<PaymentCaptured> events) {
        CompletableFuture<?>[] acks = events.stream()
                .map(e -> template.send(topic, e.accountId(), toJson(e)))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(acks).join();
    }

    public KafkaTemplate<String, String> template() {
        return template;
    }

    public static String toJson(Object o) {
        try {
            return JSON.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        factory.destroy();
    }
}
