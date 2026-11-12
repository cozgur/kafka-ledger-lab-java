package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lab.ledger.Faults;
import lab.payments.PaymentCaptured;
import lab.support.Infra;
import lab.support.Lab;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/**
 * An infrastructure outage is not a poison message. While the ledger's database is unreachable every payment fails,
 * but the payments are valid: they must wait and be applied once the database is back, not be dead-lettered after a
 * few attempts. The database sits behind Toxiproxy (own Postgres, so the shared one is never disturbed); the test
 * cuts the connection, lets the consumer exhaust more than the 3-attempt poison budget, then restores it.
 */
@Tag("isolated")
class DbOutageTest {

    static final Network NETWORK = Network.newNetwork();
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(Infra.POSTGRES_IMAGE).withNetwork(NETWORK).withNetworkAliases("pg");
    static final ToxiproxyContainer TOXIPROXY = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0").withNetwork(NETWORK);
    static Proxy proxy;

    @BeforeAll
    static void start() throws Exception {
        POSTGRES.start();
        TOXIPROXY.start();
        proxy = new ToxiproxyClient(TOXIPROXY.getHost(), TOXIPROXY.getControlPort()).createProxy("pg", "0.0.0.0:8666", "pg:5432");
    }

    @AfterAll
    static void stop() {
        TOXIPROXY.stop();
        POSTGRES.stop();
        NETWORK.close();
    }

    static Lab.Db throughProxy() {
        String url = "jdbc:postgresql://" + TOXIPROXY.getHost() + ":" + TOXIPROXY.getMappedPort(8666) + "/" + POSTGRES.getDatabaseName();
        return new Lab.Db(url, POSTGRES.getUsername(), POSTGRES.getPassword(), 1_000); // fail fast while the DB is down
    }

    @Test
    void validPaymentsWaitOutADatabaseOutageInsteadOfBeingDeadLettered() throws Exception {
        try (var lab = new Lab(1, Infra.bootstrapServers(), throughProxy())) {
            var attempts = new AtomicInteger();
            lab.startConsumer(new Faults() {
                @Override
                public void beforeApply(PaymentCaptured e, int attempt) {
                    attempts.incrementAndGet();
                }
            });
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 1, 100)));
            await().atMost(Duration.ofSeconds(20)).until(() -> lab.lag() == 0); // DB reachable: applied normally

            proxy.disable(); // database outage
            try {
                lab.producer.publish(lab.topic, IntStream.rangeClosed(2, 6).mapToObj(i -> pay("acc-1", i, 10)).toList());
                // The outage outlasts the poison budget: more processing attempts than the 3 a poison message gets.
                int before = attempts.get();
                await().atMost(Duration.ofSeconds(60)).alias("more than 3 failed attempts during the outage")
                        .until(() -> attempts.get() - before >= 5);
            } finally {
                proxy.enable(); // database back
            }

            await().atMost(Duration.ofSeconds(60)).alias("lag 0 after the outage").until(() -> lab.lag() == 0);
            assertThat(lab.dltCount()).as("valid payments dead-lettered because of an outage").isZero();
            assertThat(lab.ledger.appliedSequences("acc-1")).containsExactly(1, 2, 3, 4, 5, 6);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(150);
        }
    }
}
