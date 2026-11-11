package lab;

import static lab.support.Payments.pay;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lab.ledger.LedgerRepository;
import lab.ledger.LedgerRepository.Outcome;
import lab.support.Infra;
import org.junit.jupiter.api.Test;

/**
 * paymentId is a GLOBAL identity, but the account lock only serialises one account. Two payments with the same paymentId
 * on DIFFERENT accounts must still be serialised, or both can pass the identity check. Deterministic: session A applies
 * the first one inside an open transaction; session B's apply must wait for A, then see it and report a conflict.
 */
class ConcurrentIdentityTest {

    @Test
    void theSamePaymentIdOnTwoAccountsConcurrentlyIsAConflictNotTwoPayments() throws Exception {
        String schema = "t_" + UUID.randomUUID().toString().substring(0, 8);
        try (var ledger = new LedgerRepository(Infra.POSTGRES.getJdbcUrl(), Infra.POSTGRES.getUsername(), Infra.POSTGRES.getPassword(), schema);
             Connection a = DriverManager.getConnection(Infra.POSTGRES.getJdbcUrl() + "&currentSchema=" + schema,
                     Infra.POSTGRES.getUsername(), Infra.POSTGRES.getPassword())) {
            a.setAutoCommit(false);
            try (var st = a.prepareStatement("SELECT apply_payment(?, ?, ?, ?, ?)")) {
                st.setString(1, "p-x"); st.setString(2, "acc-1"); st.setLong(3, 10); st.setString(4, "EUR"); st.setInt(5, 2);
                var rs = st.executeQuery();
                rs.next();
                assertThat(rs.getString(1)).isEqualTo("PARKED"); // A has parked p-x, NOT committed yet
            }

            var b = CompletableFuture.supplyAsync(() -> ledger.applyPayment(pay("acc-2", 2, 20, "p-x")));
            // B must wait for A (same paymentId), not race past the identity check: it stays unfinished for a 500 ms window.
            await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                    .alias("B still waiting while A's transaction is open").until(() -> !b.isDone());

            a.commit();
            Outcome outcomeB = b.get(10, TimeUnit.SECONDS);
            assertThat(outcomeB).isEqualTo(Outcome.CONFLICT);
            assertThat(ledger.parkedSequences("acc-2")).isEmpty();
        }
    }
}
