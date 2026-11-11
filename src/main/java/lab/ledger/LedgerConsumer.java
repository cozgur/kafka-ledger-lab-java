package lab.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lab.payments.PaymentCaptured;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.MessageListener;

/** Ledger side: a Spring Kafka listener container that applies PaymentCaptured events to balances. */
public class LedgerConsumer implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();
    private final ConcurrentMessageListenerContainer<String, String> container;

    public LedgerConsumer(String bootstrapServers, String topic, String groupId, LedgerRepository ledger) {
        this(bootstrapServers, topic, groupId, ledger, Faults.NONE);
    }

    public LedgerConsumer(String bootstrapServers, String topic, String groupId, LedgerRepository ledger, Faults faults) {
        var consumerFactory = new DefaultKafkaConsumerFactory<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                // Spring Kafka commits offsets itself (AckMode below); the client's own auto-commit is off.
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                // Used when the group has no committed offset (or it's out of range): start from the oldest record.
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));

        var props = new ContainerProperties(topic);
        // DECISION (step 1, same as the TS lab): write to the DB first, then commit -> at-least-once.
        // AckMode.RECORD commits each record's offset after the listener returned for it.
        props.setAckMode(AckMode.RECORD);
        props.setMessageListener((MessageListener<String, String>) (ConsumerRecord<String, String> rec) -> {
            PaymentCaptured event = parse(rec.value());
            try {
                faults.beforeApply(event, 1);
                ledger.applyPayment(event);
                faults.afterDbWrite(event, rec.partition(), rec.offset());
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        this.container = new ConcurrentMessageListenerContainer<>(consumerFactory, props);
        container.setConcurrency(3); // one consumer thread per partition of a 3-partition topic
        container.start();
    }

    private static PaymentCaptured parse(String value) {
        try {
            return JSON.readValue(value, PaymentCaptured.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("cannot parse PaymentCaptured", e);
        }
    }

    public void stop() {
        container.stop();
    }

    @Override
    public void close() {
        stop();
    }
}
