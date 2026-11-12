package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lab.payments.PaymentCaptured;
import lab.support.Infra;
import lab.support.Lab;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A real process kill (SIGKILL) of the ledger service inside the at-least-once loss window: records are committed in
 * the DB but their offsets are not. The service runs as a separate JVM; a test-only setting makes every listener thread
 * block right after the DB write once N records were processed, and the test kills the JVM there. No graceful shutdown,
 * no offset commit on the way out. A restarted service must redeliver and absorb those records: nothing lost, nothing
 * applied twice, order kept, and redeliveries not mistaken for conflicts.
 */
@Tag("isolated")
class ProcessKillTest {

    static final List<String> ACCOUNTS = IntStream.range(0, 12).mapToObj(i -> "acc-" + i).toList();

    static List<PaymentCaptured> events() {
        List<PaymentCaptured> e = new ArrayList<>();
        for (int i = 1; i <= 25; i++) for (String a : ACCOUNTS) e.add(pay(a, i, i));
        return e; // 300 events
    }

    static Process startLedgerService(Lab lab, int pauseAt, String logName) throws Exception {
        Path log = Path.of("build/test-logs/" + logName);
        Files.createDirectories(log.getParent());
        var cmd = List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("lab.serviceClasspath"),
                "lab.LedgerApplication",
                "--lab.kafka.bootstrap-servers=" + Infra.bootstrapServers(),
                "--lab.kafka.topic=" + lab.topic,
                "--lab.kafka.group-id=" + lab.groupId,
                "--lab.db.url=" + Infra.POSTGRES.getJdbcUrl(),
                "--lab.db.user=" + Infra.POSTGRES.getUsername(),
                "--lab.db.password=" + Infra.POSTGRES.getPassword(),
                "--lab.db.schema=t_" + lab.id,
                "--lab.fault.pause-after-db-write-at=" + pauseAt);
        return new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    @Test
    void sigkillAfterTheDbWriteBeforeTheOffsetCommitLosesNothingAndAppliesNothingTwice() throws Exception {
        assertThat(System.getProperty("lab.serviceClasspath")).as("service classpath from Gradle").isNotBlank();
        try (var lab = new Lab(3)) {
            var events = events();
            Process first = startLedgerService(lab, 120, "ledger-process-" + lab.id + "-first.log");
            Process second = null;
            try {
                lab.producer.publish(lab.topic, events);

                // Wait (on DB state, not time) until the service has applied >= 120 records and every thread is parked
                // after its DB write: the applied count stops moving while the committed offsets lag behind it.
                await().atMost(Duration.ofSeconds(60)).alias("service applied >= 120").until(() -> lab.ledger.appliedCount() >= 120);
                await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(10)).alias("all threads parked")
                        .until(lab.ledger::appliedCount, n -> n == lab.ledger.appliedCount());

                long appliedAtKill = lab.ledger.appliedCount();
                long committedAtKill = lab.committedTotal();
                // Precondition: the kill lands inside the loss window. Some records are durable in the DB but not
                // committed in Kafka, so the restarted service MUST see them again.
                assertThat(appliedAtKill).as("applied in DB at kill").isGreaterThan(committedAtKill);
                assertThat(appliedAtKill).as("stopped mid-stream").isLessThan(events.size());

                first.destroyForcibly(); // SIGKILL on Unix: no shutdown hooks, no commit on close
                assertThat(first.waitFor(30, TimeUnit.SECONDS)).isTrue();

                second = startLedgerService(lab, 0, "ledger-process-" + lab.id + "-second.log");
                await().atMost(Duration.ofSeconds(90)).alias("lag 0 after restart").until(() -> lab.lag() == 0);

                assertThat(lab.ledger.appliedCount()).isEqualTo(events.size());
                var expected = IntStream.rangeClosed(1, 25).boxed().toList();
                for (String a : ACCOUNTS) {
                    assertThat(lab.ledger.appliedSequences(a)).as("apply order for %s", a).isEqualTo(expected);
                    assertThat(lab.ledger.balanceOf(a)).isEqualTo(325);
                }
                assertThat(lab.dltCount()).as("redeliveries are duplicates, not conflicts").isZero();
            } finally {
                first.destroyForcibly();
                if (second != null) second.destroyForcibly();
            }
        }
    }
}
