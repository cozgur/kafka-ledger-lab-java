package lab.support;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import lab.ledger.LedgerConsumer;
import lab.ledger.LedgerRepository;
import lab.payments.PaymentsProducer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.RecordsToDelete;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import static org.awaitility.Awaitility.await;

/** Per-test fixture: a fresh topic, DLT, consumer group and DB schema, so tests never share state. */
public final class Lab implements AutoCloseable {

    public final String id = UUID.randomUUID().toString().substring(0, 8);
    public final String topic = "payments.captured." + id;
    public final String dltTopic = topic + ".dlt";
    public final String groupId = "ledger-" + id;
    public final int partitions;
    public final String bootstrap;
    public final Admin admin;
    public final LedgerRepository ledger;
    public final PaymentsProducer producer;
    private final List<LedgerConsumer> consumers = new CopyOnWriteArrayList<>();

    public Lab(int partitions) {
        this(partitions, Infra.bootstrapServers());
    }

    /** A lab on a specific broker (e.g. a dedicated one for a test that pauses it). */
    public Lab(int partitions, String bootstrap) {
        this.partitions = partitions;
        this.bootstrap = bootstrap;
        this.admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
        try {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1), new NewTopic(dltTopic, 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        awaitLeaders(admin, topic, partitions);
        awaitLeaders(admin, dltTopic, 1);
        this.ledger = new LedgerRepository(Infra.POSTGRES.getJdbcUrl(), Infra.POSTGRES.getUsername(), Infra.POSTGRES.getPassword(), "t_" + id);
        this.producer = new PaymentsProducer(bootstrap);
    }

    public Lab() {
        this(3);
    }

    public LedgerConsumer startConsumer() {
        return startConsumer(lab.ledger.Faults.NONE);
    }

    public LedgerConsumer startConsumer(lab.ledger.Faults faults) {
        var c = new LedgerConsumer(bootstrap, topic, groupId, ledger,
                new LedgerConsumer.DltPolicy(dltTopic, 3, 100), faults);
        consumers.add(c);
        return c;
    }

    /** Sum over partitions of (log-end offset - committed offset) for our group, via the admin API. */
    public long lag() {
        try {
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            for (int p = 0; p < partitions; p++) latest.put(new TopicPartition(topic, p), OffsetSpec.latest());
            Map<TopicPartition, ListOffsetsResultInfo> ends = admin.listOffsets(latest).all().get();
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            long lag = 0;
            for (var e : ends.entrySet()) {
                OffsetAndMetadata c = committed.get(e.getKey());
                lag += e.getValue().offset() - (c == null ? 0 : c.offset());
            }
            return lag;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Rewind the ledger group to the earliest retained offset with the Admin API (the group must have no active members;
     * we wait until it is empty). Java's Admin client can do this directly; the TS lab had to shell out to the CLI.
     */
    public void resetGroupToEarliest() throws Exception {
        await().atMost(Duration.ofSeconds(30)).alias("group " + groupId + " empty").until(() ->
                admin.describeConsumerGroups(List.of(groupId)).all().get().get(groupId).groupState()
                        .toString().equalsIgnoreCase("EMPTY"));
        Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();
        for (int p = 0; p < partitions; p++) earliest.put(new TopicPartition(topic, p), OffsetSpec.earliest());
        Map<TopicPartition, OffsetAndMetadata> target = new HashMap<>();
        admin.listOffsets(earliest).all().get().forEach((tp, info) -> target.put(tp, new OffsetAndMetadata(info.offset())));
        admin.alterConsumerGroupOffsets(groupId, target).all().get();
    }

    /** Simulate retention: delete every record of {@code partition} below {@code beforeOffset}. */
    public void deleteRecordsBefore(int partition, long beforeOffset) throws Exception {
        admin.deleteRecords(Map.of(new TopicPartition(topic, partition), RecordsToDelete.beforeOffset(beforeOffset))).all().get();
    }

    /**
     * Wait until every partition's leader answers. createTopics() returns before the broker has loaded leadership: the
     * first produce then got NOT_LEADER_OR_FOLLOWER and the idempotent producer's retries got OUT_OF_ORDER_SEQUENCE_NUMBER
     * for the whole 120s delivery timeout (the "no ack" flake, seen in ConsistencyTest and RestartTest; client log).
     * ListOffsets is served by the partition leader, so a successful call means the leader is ready.
     */
    public static void awaitLeaders(Admin admin, String topic, int partitions) {
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        for (int p = 0; p < partitions; p++) latest.put(new TopicPartition(topic, p), OffsetSpec.latest());
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50)).ignoreExceptions()
                .alias("leaders of " + topic).until(() -> admin.listOffsets(latest).all().get().size() == partitions);
    }

    /** Committed offset of our group for one partition on the broker (-1 = nothing committed). */
    public long committedOffset(int partition) throws Exception {
        var c = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().get(new TopicPartition(topic, partition));
        return c == null ? -1 : c.offset();
    }

    /** Publish raw key/value strings, bypassing the typed producer (used for malformed payloads). */
    public void publishRaw(List<String[]> keyValues) {
        try (var p = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            for (String[] kv : keyValues) p.send(new ProducerRecord<>(topic, kv[0], kv[1]));
            p.flush();
        }
    }

    /** Number of messages in the DLT (from broker offsets). */
    public long dltCount() throws Exception {
        var tp = new TopicPartition(dltTopic, 0);
        return admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get().get(tp).offset();
    }

    /** Read the first {@code n} DLT records. */
    public List<ConsumerRecord<String, String>> readDlt(int n) {
        return read(dltTopic, n);
    }

    /** Read every record currently in the main topic with an independent consumer group (an auditor). */
    public List<ConsumerRecord<String, String>> readAll() throws Exception {
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        for (int p = 0; p < partitions; p++) latest.put(new TopicPartition(topic, p), OffsetSpec.latest());
        long total = admin.listOffsets(latest).all().get().values().stream().mapToLong(ListOffsetsResultInfo::offset).sum();
        return read(topic, (int) total);
    }

    private List<ConsumerRecord<String, String>> read(String name, int n) {
        List<ConsumerRecord<String, String>> got = new ArrayList<>();
        try (var c = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "reader-" + id + "-" + UUID.randomUUID().toString().substring(0, 4),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            c.subscribe(List.of(name));
            await().atMost(Duration.ofSeconds(20)).alias(n + " record(s) from " + name).until(() -> {
                c.poll(Duration.ofMillis(200)).forEach(got::add);
                return got.size() >= n;
            });
        }
        return got;
    }

    /** A header value as UTF-8 text (null if absent). */
    public static String header(ConsumerRecord<?, ?> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** Number of partitions of the main topic that hold at least one record. */
    public long nonEmptyPartitions() throws Exception {
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        for (int p = 0; p < partitions; p++) latest.put(new TopicPartition(topic, p), OffsetSpec.latest());
        return admin.listOffsets(latest).all().get().values().stream().filter(i -> i.offset() > 0).count();
    }

    @Override
    public void close() {
        consumers.forEach(LedgerConsumer::close);
        producer.close();
        ledger.close();
        admin.close();
    }

}
