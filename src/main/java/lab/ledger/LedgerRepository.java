package lab.ledger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lab.payments.PaymentCaptured;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Ledger persistence. Each instance works inside its own Postgres schema, so tests can run in parallel
 * against one database without seeing each other's rows.
 */
public class LedgerRepository implements AutoCloseable {

    private final HikariDataSource dataSource;
    protected final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    public LedgerRepository(String jdbcUrl, String user, String password, String schema) {
        if (!schema.matches("[a-z_][a-z0-9_]*")) throw new IllegalArgumentException("unsafe schema name: " + schema);
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(user);
        cfg.setPassword(password);
        cfg.setMaximumPoolSize(6);
        cfg.setSchema(schema); // search_path for every pooled connection
        // Create the schema with a throwaway connection before the pool starts using it.
        try (var bootstrap = new HikariDataSource(withoutSchema(cfg))) {
            new JdbcTemplate(bootstrap).execute("CREATE SCHEMA IF NOT EXISTS " + schema);
        }
        this.dataSource = new HikariDataSource(cfg);
        this.jdbc = new JdbcTemplate(dataSource);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS balances (
                  account_id    text PRIMARY KEY,
                  currency      text   NOT NULL,
                  balance_minor bigint NOT NULL,
                  last_sequence int    NOT NULL DEFAULT 0  -- highest per-account sequence applied so far
                )""");
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS applied_payments (
                  payment_id   text PRIMARY KEY,   -- the idempotency key
                  apply_order  bigserial,          -- global order in which the ledger applied events
                  was_parked   boolean NOT NULL DEFAULT false,
                  account_id   text   NOT NULL,
                  amount_minor bigint NOT NULL,
                  sequence     int    NOT NULL,
                  applied_at   timestamptz NOT NULL DEFAULT now()
                )""");
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS parked_events (
                  account_id text NOT NULL,
                  sequence   int  NOT NULL,
                  payment_id text NOT NULL,
                  amount_minor bigint NOT NULL,
                  currency   text NOT NULL,
                  parked_at  timestamptz NOT NULL DEFAULT now(),
                  PRIMARY KEY (account_id, sequence)
                )""");
    }

    private static HikariConfig withoutSchema(HikariConfig cfg) {
        HikariConfig c = new HikariConfig();
        c.setJdbcUrl(cfg.getJdbcUrl());
        c.setUsername(cfg.getUsername());
        c.setPassword(cfg.getPassword());
        c.setMaximumPoolSize(1);
        return c;
    }

    /**
     * Apply one event idempotently and in per-account sequence order, in ONE transaction:
     * 1. lock the account row (concurrent applies for one account are serialised)
     * 2. paymentId seen (applied or parked) -> same content DUPLICATE, different content CONFLICT
     * 3. new paymentId on a taken slot (sequence <= last, or already parked) -> SEQUENCE_CONFLICT
     * 4. sequence >  last_sequence + 1 -> PARKED (gap: wait for the missing event)
     * 5. sequence == last_sequence + 1 -> APPLIED, then drain parked successors in order
     */
    public Outcome applyPayment(PaymentCaptured e) {
        return tx.execute(status -> {
            jdbc.update("INSERT INTO balances (account_id, currency, balance_minor) VALUES (?, ?, 0) ON CONFLICT DO NOTHING",
                    e.accountId(), e.currency());
            int last = jdbc.queryForObject("SELECT last_sequence FROM balances WHERE account_id = ? FOR UPDATE", Integer.class, e.accountId());
            // Identity: a paymentId seen before (applied OR parked): same content = redelivery, different = conflict.
            var applied = jdbc.queryForList("SELECT account_id, amount_minor, sequence FROM applied_payments WHERE payment_id = ?", e.paymentId());
            if (!applied.isEmpty()) return sameContent(applied.get(0), e) ? Outcome.DUPLICATE : Outcome.CONFLICT;
            var parked = jdbc.queryForList("SELECT account_id, amount_minor, sequence FROM parked_events WHERE payment_id = ?", e.paymentId());
            if (!parked.isEmpty()) return sameContent(parked.get(0), e) ? Outcome.DUPLICATE : Outcome.CONFLICT;
            // Slot: a NEW paymentId claiming a sequence that is already taken (applied OR parked) -> sequence conflict.
            if (e.sequence() <= last) return Outcome.SEQUENCE_CONFLICT;
            if (e.sequence() > last + 1) {
                if (Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS (SELECT 1 FROM parked_events WHERE account_id = ? AND sequence = ?)",
                        Boolean.class, e.accountId(), e.sequence()))) {
                    return Outcome.SEQUENCE_CONFLICT;
                }
                jdbc.update("INSERT INTO parked_events (account_id, sequence, payment_id, amount_minor, currency) VALUES (?, ?, ?, ?, ?)",
                        e.accountId(), e.sequence(), e.paymentId(), e.amountMinor(), e.currency());
                return Outcome.PARKED;
            }
            apply(e, false);
            last = e.sequence();
            for (;;) { // drain parked successors in order
                var next = jdbc.query("""
                        DELETE FROM parked_events WHERE account_id = ? AND sequence = ?
                        RETURNING payment_id, amount_minor, currency""",
                        (rs, i) -> new PaymentCaptured(rs.getString(1), e.accountId(), rs.getLong(2), rs.getString(3), 0),
                        e.accountId(), last + 1);
                if (next.isEmpty()) break;
                last++;
                var p = next.get(0);
                apply(new PaymentCaptured(p.paymentId(), p.accountId(), p.amountMinor(), p.currency(), last), true);
            }
            return Outcome.APPLIED;
        });
    }

    private static boolean sameContent(java.util.Map<String, Object> row, PaymentCaptured e) {
        return e.accountId().equals(row.get("account_id"))
                && e.amountMinor() == ((Number) row.get("amount_minor")).longValue()
                && e.sequence() == ((Number) row.get("sequence")).intValue();
    }

    private void apply(PaymentCaptured e, boolean wasParked) {
        jdbc.update("INSERT INTO applied_payments (payment_id, account_id, amount_minor, sequence, was_parked) VALUES (?, ?, ?, ?, ?)",
                e.paymentId(), e.accountId(), e.amountMinor(), e.sequence(), wasParked);
        jdbc.update("UPDATE balances SET balance_minor = balance_minor + ?, last_sequence = ? WHERE account_id = ?",
                e.amountMinor(), e.sequence(), e.accountId());
    }

    public enum Outcome { APPLIED, DUPLICATE, CONFLICT, SEQUENCE_CONFLICT, PARKED }

    public int appliedCount() {
        return jdbc.queryForObject("SELECT count(*) FROM applied_payments", Integer.class);
    }

    /** All balances sorted by account: a comparable snapshot of the ledger state ("account:balance:lastSequence"). */
    public java.util.List<String> snapshot() {
        return jdbc.query("SELECT account_id, balance_minor, last_sequence FROM balances ORDER BY account_id",
                (rs, i) -> rs.getString(1) + ":" + rs.getLong(2) + ":" + rs.getInt(3));
    }

    /** Accounts whose balance differs from the sum of their applied payments (should always be empty). */
    public java.util.List<String> internalMismatches() {
        return jdbc.query("""
                SELECT b.account_id FROM balances b LEFT JOIN applied_payments a ON a.account_id = b.account_id
                GROUP BY b.account_id, b.balance_minor HAVING b.balance_minor <> COALESCE(SUM(a.amount_minor), 0)""",
                (rs, i) -> rs.getString(1));
    }

    /** Balance per account. */
    public java.util.Map<String, Long> balances() {
        java.util.Map<String, Long> m = new java.util.TreeMap<>();
        jdbc.query("SELECT account_id, balance_minor FROM balances", rs -> { m.put(rs.getString(1), rs.getLong(2)); });
        return m;
    }

    /** Drop all ledger state (used before a replay rebuild). */
    public void wipe() {
        jdbc.execute("TRUNCATE balances, applied_payments, parked_events");
    }

    /** Sequences of an account in the order the ledger applied them. */
    public java.util.List<Integer> appliedSequences(String accountId) {
        return jdbc.queryForList("SELECT sequence FROM applied_payments WHERE account_id = ? ORDER BY apply_order", Integer.class, accountId);
    }

    /** Sequences of an account that waited in parked_events before being applied. */
    public java.util.List<Integer> sequencesAppliedFromPark(String accountId) {
        return jdbc.queryForList("SELECT sequence FROM applied_payments WHERE account_id = ? AND was_parked ORDER BY apply_order", Integer.class, accountId);
    }

    public java.util.List<Integer> parkedSequences(String accountId) {
        return jdbc.queryForList("SELECT sequence FROM parked_events WHERE account_id = ? ORDER BY sequence", Integer.class, accountId);
    }

    /** Balance in minor units, or null if the account has no row. */
    public Long balanceOf(String accountId) {
        return jdbc.query("SELECT balance_minor FROM balances WHERE account_id = ?",
                rs -> rs.next() ? rs.getLong(1) : null, accountId);
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
