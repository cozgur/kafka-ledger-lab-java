package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import lab.payments.PaymentCaptured;
import lab.payments.PaymentsProducer;
import lab.support.Infra;
import lab.support.Lab;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Broker outage on a single broker: the broker is frozen (docker pause) while payments are being produced.
 * Sends must not complete during the outage; after it they all complete, the ledger is right, and the log holds no
 * duplicate records from the producer's retries (idempotent producer). The producer gets a 1s request timeout so it
 * really retries during the 3s outage: measured, with enable.idempotence=false the same run wrote 90 records instead
 * of 60 (30 duplicates); with the default 30s timeout no retry happened and the check proved nothing. Uses its OWN broker so pausing it does not
 * disturb the other test classes running in parallel. Out of scope: multi-broker failover, ISR, leader election.
 *
 * Runs in the separate `isolatedTest` task, not next to the main suite: in one of 20 runs a producer in
 * ConsistencyTest (shared broker) got no ack for 120s while this test was pausing its own broker. The root cause was
 * not proven, but Docker-level fault injection should not share the machine with other tests.
 */
@Tag("isolated")
class BrokerOutageTest {

    static final KafkaContainer BROKER = new KafkaContainer(Infra.KAFKA_IMAGE);

    @BeforeAll
    static void start() {
        BROKER.start();
    }

    @AfterAll
    static void stop() {
        BROKER.stop();
    }

    static void pause() {
        BROKER.getDockerClient().pauseContainerCmd(BROKER.getContainerId()).exec();
    }

    static void unpause() {
        BROKER.getDockerClient().unpauseContainerCmd(BROKER.getContainerId()).exec();
    }

    @Test
    void brokerFrozenWhileProducingNoLossNoDuplicateRecordsLedgerCorrect() throws Exception {
        try (var lab = new Lab(3, BROKER.getBootstrapServers())) {
            lab.startConsumer();
            var accounts = List.of("acc-a", "acc-b", "acc-c");
            lab.producer.publish(lab.topic, IntStream.rangeClosed(1, 10).boxed()
                    .flatMap(i -> accounts.stream().map(a -> pay(a, i, i))).toList());
            await().atMost(Duration.ofSeconds(20)).until(() -> lab.lag() == 0);

            // A producer that retries during the outage (request timeout 1s < 3s pause).
            var retrying = new PaymentsProducer(lab.bootstrap, java.util.Map.of(
                    org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000,
                    org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 90_000));
            // Before the outage: fetch metadata (else send() blocks on it) and open the produce connection with a warm-up
            // record on a side topic, so the outage hits in-flight produce requests, which is when retries duplicate.
            retrying.template().partitionsFor(lab.topic);
            retrying.template().send(lab.topic + ".warmup", "warmup", "warmup").get();
            List<CompletableFuture<?>> inFlight = new ArrayList<>();
            pause();
            try {
                for (int i = 11; i <= 20; i++) {
                    for (String a : accounts) {
                        PaymentCaptured e = pay(a, i, i);
                        inFlight.add(retrying.template().send(lab.topic, e.accountId(), PaymentsProducer.toJson(e)));
                    }
                }
                // While the broker is frozen, no send can be acknowledged (the producer buffers and retries).
                await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5))
                        .until(() -> inFlight.stream().noneMatch(CompletableFuture::isDone));
            } finally {
                unpause();
            }

            CompletableFuture.allOf(inFlight.toArray(CompletableFuture[]::new)).get(java.util.concurrent.TimeUnit.SECONDS.toSeconds(90), java.util.concurrent.TimeUnit.SECONDS);
            retrying.close();
            await().atMost(Duration.ofSeconds(60)).until(() -> lab.lag() == 0);

            assertThat(lab.readAll()).as("records in the log: no duplicates from producer retries").hasSize(60);
            assertThat(lab.ledger.appliedCount()).isEqualTo(60);
            for (String a : accounts) {
                assertThat(lab.ledger.appliedSequences(a)).isEqualTo(IntStream.rangeClosed(1, 20).boxed().toList());
                assertThat(lab.ledger.balanceOf(a)).isEqualTo(210);
            }
        }
    }
}
