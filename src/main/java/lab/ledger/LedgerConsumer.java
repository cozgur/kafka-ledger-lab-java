package lab.ledger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import lab.ledger.PaymentValidator.InvalidPayloadException;
import lab.payments.PaymentCaptured;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.util.backoff.FixedBackOff;

/** Ledger side: a Spring Kafka listener container that applies PaymentCaptured events to balances. */
public class LedgerConsumer implements AutoCloseable {

    private final ConcurrentMessageListenerContainer<String, String> container;
    private final DefaultKafkaProducerFactory<String, String> dltProducerFactory;

    /** Dead-letter policy: where poison messages go and how many processing attempts they get. */
    public record DltPolicy(String topic, int maxAttempts, long backoffMs) {}

    public LedgerConsumer(String bootstrapServers, String topic, String groupId, LedgerRepository ledger, DltPolicy dlt, Faults faults) {
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
        // DECISION (same as the TS lab): write to the DB first, then commit -> at-least-once.
        // AckMode.RECORD commits each record's offset after the listener returned for it.
        props.setAckMode(AckMode.RECORD);
        props.setDeliveryAttemptHeader(true); // exposes the attempt number to the listener
        // On stop(), finish the CURRENT record and stop. The default (false) first processes every record already
        // fetched by the last poll (up to max.poll.records), which made a "graceful stop" drain 200/200 events in
        // RestartTest. Unprocessed records are simply redelivered after the restart (at-least-once).
        props.setStopImmediate(true);
        props.setMessageListener((MessageListener<String, String>) (ConsumerRecord<String, String> rec) -> {
            PaymentCaptured event = PaymentValidator.parse(rec.value()); // InvalidPayloadException is not retried
            try {
                faults.beforeApply(event, attempt(rec));
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

        // Spring's DEFAULT error handler retries 10 times, then logs and SKIPS the record: the payment would be
        // silently lost (shown red in DeadLetterTest). Explicit policy instead: bounded retries, then the DLT.
        this.dltProducerFactory = new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all"));
        var recoverer = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(dltProducerFactory),
                (rec, ex) -> new TopicPartition(dlt.topic(), 0));
        // Original key/value bytes are kept; diagnostics go to headers. The recoverer publishes synchronously and only
        // then is the original offset committed, so a crash in between duplicates the DLT entry but never loses it.
        recoverer.setHeadersFunction((rec, ex) -> {
            Throwable cause = rootCause(ex);
            boolean invalid = cause instanceof InvalidPayloadException;
            var h = new RecordHeaders();
            h.add("dlt.original.topic", rec.topic().getBytes(StandardCharsets.UTF_8));
            h.add("dlt.original.partition", String.valueOf(rec.partition()).getBytes(StandardCharsets.UTF_8));
            h.add("dlt.original.offset", String.valueOf(rec.offset()).getBytes(StandardCharsets.UTF_8));
            h.add("dlt.reason", (invalid ? "invalid-payload" : "processing-failed").getBytes(StandardCharsets.UTF_8));
            h.add("dlt.attempts", String.valueOf(invalid ? 1 : attempt(rec)).getBytes(StandardCharsets.UTF_8));
            h.add("dlt.error", String.valueOf(cause.getMessage()).getBytes(StandardCharsets.UTF_8));
            h.add("dlt.failed.at", Instant.now().toString().getBytes(StandardCharsets.UTF_8));
            return h;
        });
        var errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(dlt.backoffMs(), dlt.maxAttempts() - 1));
        errorHandler.addNotRetryableExceptions(InvalidPayloadException.class);
        container.setCommonErrorHandler(errorHandler);
        container.start();
    }

    /** Delivery attempt (1-based) from Spring's header. */
    static int attempt(ConsumerRecord<?, ?> rec) {
        var h = rec.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
        return h == null ? 1 : ByteBuffer.wrap(h.value()).getInt();
    }

    static Throwable rootCause(Throwable t) {
        Throwable c = t instanceof ListenerExecutionFailedException && t.getCause() != null ? t.getCause() : t;
        while (c.getCause() != null && c.getCause() != c && !(c instanceof InvalidPayloadException)) c = c.getCause();
        return c;
    }

    public void stop() {
        container.stop();
    }

    @Override
    public void close() {
        stop();
        dltProducerFactory.destroy();
    }
}
