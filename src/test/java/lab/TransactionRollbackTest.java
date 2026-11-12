package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lab.ledger.Faults;
import lab.payments.PaymentCaptured;
import lab.support.Infra;
import lab.support.Lab;
import org.junit.jupiter.api.Test;

/**
 * A failure in the middle of the ledger transaction leaves no partial state. A trigger (only in this test's schema)
 * fails the balance update once: at that point the dedupe row is already inserted. Everything must roll back
 * together (no "marked applied but no money moved", which would make the redelivery look like a duplicate and lose the
 * payment), and the retry must apply the payment once. The fail-once counter is a sequence, because nextval() is not
 * rolled back with the transaction.
 */
class TransactionRollbackTest {

    static void failBalanceUpdateOnce(String schema) throws Exception {
        try (Connection c = DriverManager.getConnection(Infra.POSTGRES.getJdbcUrl() + "&currentSchema=" + schema,
                Infra.POSTGRES.getUsername(), Infra.POSTGRES.getPassword()); var st = c.createStatement()) {
            st.execute("CREATE SEQUENCE fail_once");
            st.execute("""
                    CREATE FUNCTION fail_balance_update_once() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                      IF nextval('fail_once') = 1 THEN
                        RAISE EXCEPTION 'simulated failure inside the ledger transaction';
                      END IF;
                      RETURN NEW;
                    END $$""");
            st.execute("""
                    CREATE TRIGGER fail_balance_update_once AFTER UPDATE ON balances FOR EACH ROW
                    WHEN (NEW.balance_minor IS DISTINCT FROM OLD.balance_minor) EXECUTE FUNCTION fail_balance_update_once()""");
        }
    }

    @Test
    void aFailureAfterTheDedupeInsertRollsBackEverythingAndTheRetryAppliesOnce() throws Exception {
        try (var lab = new Lab(1)) {
            failBalanceUpdateOnce("t_" + lab.id);
            var atRetry = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            lab.startConsumer(new Faults() {
                @Override
                public void beforeApply(PaymentCaptured e, int attempt) throws Exception {
                    if (attempt == 2) { // the first attempt failed inside the transaction
                        atRetry.countDown();
                        release.await(30, TimeUnit.SECONDS);
                    }
                }
            });
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 1, 100)));
            try {
                assertThat(atRetry.await(30, TimeUnit.SECONDS)).as("the first attempt failed and a retry started").isTrue();
                // Between the failed attempt and the retry: no trace of the payment anywhere.
                assertThat(lab.ledger.appliedCount()).as("dedupe rows after the failure").isZero();
                assertThat(lab.ledger.snapshot()).as("balances after the failure").isEmpty();
            } finally {
                release.countDown();
            }

            await().atMost(Duration.ofSeconds(20)).until(() -> lab.lag() == 0);
            assertThat(lab.ledger.appliedCount()).isEqualTo(1);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(100);
            assertThat(lab.dltCount()).isZero();
        }
    }
}
