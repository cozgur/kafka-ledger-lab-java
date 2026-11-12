package lab.ledger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;
import lab.ledger.PaymentValidator.InvalidPayloadException;
import lab.payments.PaymentCaptured;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
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
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
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
        // DECISION: write to the DB first, then commit -> at-least-once.
        // AckMode.BATCH commits the offsets of a poll's records after the listener processed them all (on an error,
        // the error handler commits the ones before the failed record). Measured: AckMode.RECORD (one synchronous
        // commit per record) took 18.9s for a 5,000-event burst; BATCH took 3.7s. Unlike the TS lab, the Java
        // bottleneck was the per-record commit, not the database.
        props.setAckMode(AckMode.BATCH);
        props.setDeliveryAttemptHeader(true); // exposes the attempt number to the listener
        // On stop(), finish the CURRENT record and stop. The default (false) first processes every record already
        // fetched by the last poll (up to max.poll.records), which made a "graceful stop" drain 200/200 events in
        // RestartTest. Unprocessed records are simply redelivered after the restart (at-least-once).
        props.setStopImmediate(true);
        props.setMessageListener((MessageListener<String, String>) (ConsumerRecord<String, String> rec) -> {
            PaymentCaptured event = PaymentValidator.parse(rec.value()); // InvalidPayloadException is not retried
            try {
                faults.beforeApply(event, attempt(rec));
                var outcome = ledger.applyPayment(event);
                // Conflicts are not retryable and must never be applied or silently dropped: to the DLT, for humans.
                if (outcome == LedgerRepository.Outcome.CONFLICT) throw new ConflictException("payment-id-conflict",
                        "paymentId " + event.paymentId() + " reused with different content");
                if (outcome == LedgerRepository.Outcome.SEQUENCE_CONFLICT) throw new ConflictException("sequence-conflict",
                        "sequence " + event.sequence() + " of " + event.accountId() + " is already taken by another payment");
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
            boolean conflict = cause instanceof ConflictException;
            String reason = invalid ? "invalid-payload" : conflict ? ((ConflictException) cause).reason : "processing-failed";
            var h = new RecordHeaders();
            h.add("dlt.original.topic", rec.topic().getBytes(StandardCharsets.UTF_8));
            h.add("dlt.original.partition", String.valueOf(rec.partition()).getBytes(StandardCharsets.UTF_8));
            h.add("dlt.original.offset", String.valueOf(rec.offset()).getBytes(StandardCharsets.UTF_8));
            h.add("dlt.reason", reason.getBytes(StandardCharsets.UTF_8));
            h.add("dlt.attempts", String.valueOf(invalid || conflict ? 1 : attempt(rec)).getBytes(StandardCharsets.UTF_8));
            h.add("dlt.error", String.valueOf(cause.getMessage()).getBytes(StandardCharsets.UTF_8));
            h.add("dlt.failed.at", Instant.now().toString().getBytes(StandardCharsets.UTF_8));
            return h;
        });
        var errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(dlt.backoffMs(), dlt.maxAttempts() - 1));
        errorHandler.addNotRetryableExceptions(InvalidPayloadException.class, ConflictException.class);
        // An infrastructure outage is not a poison message: while the database is unreachable EVERY payment fails, and
        // dead-lettering them would turn a short outage into a manual repair job (shown red in DbOutageTest). Such errors
        // are retried without limit, backing off up to 5s; the partition waits, order is kept, nothing goes to the DLT.
        errorHandler.setBackOffFunction((rec, ex) -> isInfrastructureFailure(ex) ? INFRASTRUCTURE_BACKOFF.get() : null);
        container.setCommonErrorHandler(errorHandler);
        container.start();
    }

    private static final Supplier<BackOff> INFRASTRUCTURE_BACKOFF = () -> {
        var backOff = new ExponentialBackOff(200, 2.0);
        backOff.setMaxInterval(5_000);
        backOff.setMaxElapsedTime(Long.MAX_VALUE); // keep trying until the database is back
        return backOff;
    };

    /** True if the failure means "the database (or the connection to it) is unavailable", not "this record is bad". */
    static boolean isInfrastructureFailure(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof TransientDataAccessException || t instanceof DataAccessResourceFailureException
                    || t instanceof SQLTransientException || t instanceof SQLRecoverableException
                    || t instanceof SQLNonTransientConnectionException) {
                return true;
            }
            // SQLState class 08 = connection exception; 57P = operator intervention (e.g. the server is shutting down)
            if (t instanceof SQLException sql && sql.getSQLState() != null
                    && (sql.getSQLState().startsWith("08") || sql.getSQLState().startsWith("57P"))) {
                return true;
            }
        }
        return false;
    }

    /** Delivery attempt (1-based) from Spring's header. */
    static int attempt(ConsumerRecord<?, ?> rec) {
        var h = rec.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
        return h == null ? 1 : ByteBuffer.wrap(h.value()).getInt();
    }

    static Throwable rootCause(Throwable t) {
        Throwable c = t instanceof ListenerExecutionFailedException && t.getCause() != null ? t.getCause() : t;
        while (c.getCause() != null && c.getCause() != c && !(c instanceof InvalidPayloadException) && !(c instanceof ConflictException)) c = c.getCause();
        return c;
    }

    /** A payment that conflicts with an earlier one (identity or sequence slot). Not retryable. */
    public static final class ConflictException extends RuntimeException {
        final String reason;

        ConflictException(String reason, String message) {
            super(message);
            this.reason = reason;
        }
    }

    /** Partitions currently assigned to this instance (across its consumer threads). */
    public int assignedPartitionCount() {
        var assigned = container.getAssignedPartitions();
        return assigned == null ? 0 : assigned.size();
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
