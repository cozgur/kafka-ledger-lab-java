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

/** Per-test fixture: a fresh topic, DLT, consumer group and DB schema, so tests never share state. */
public final class Lab implements AutoCloseable {

    public final String id = UUID.randomUUID().toString().substring(0, 8);
    public final String topic = "payments.captured." + id;
    public final String dltTopic = topic + ".dlt";
    public final String groupId = "ledger-" + id;
    public final int partitions;
    public final Admin admin;
    public final LedgerRepository ledger;
    public final PaymentsProducer producer;
    private final List<LedgerConsumer> consumers = new CopyOnWriteArrayList<>();

    public Lab(int partitions) {
        this.partitions = partitions;
        this.admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Infra.bootstrapServers()));
        try {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1), new NewTopic(dltTopic, 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        this.ledger = new LedgerRepository(Infra.POSTGRES.getJdbcUrl(), Infra.POSTGRES.getUsername(), Infra.POSTGRES.getPassword(), "t_" + id);
        this.producer = new PaymentsProducer(Infra.bootstrapServers());
    }

    public Lab() {
        this(3);
    }

    public LedgerConsumer startConsumer() {
        var c = new LedgerConsumer(Infra.bootstrapServers(), topic, groupId, ledger);
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

    @Override
    public void close() {
        consumers.forEach(LedgerConsumer::close);
        producer.close();
        ledger.close();
        admin.close();
    }

}
