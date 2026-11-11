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
     * Idempotent apply: the dedupe row and the balance change commit in ONE transaction, so a crash can never leave
     * "marked applied but balance unchanged" (lost payment) or "balance changed but not marked" (double-apply).
     */
    public Outcome applyPayment(PaymentCaptured e) {
        return tx.execute(status -> {
            int inserted = jdbc.update("""
                    INSERT INTO applied_payments (payment_id, account_id, amount_minor, sequence) VALUES (?, ?, ?, ?)
                    ON CONFLICT (payment_id) DO NOTHING""", e.paymentId(), e.accountId(), e.amountMinor(), e.sequence());
            if (inserted == 0) return Outcome.DUPLICATE;
            jdbc.update("""
                    INSERT INTO balances (account_id, currency, balance_minor) VALUES (?, ?, ?)
                    ON CONFLICT (account_id) DO UPDATE SET balance_minor = balances.balance_minor + EXCLUDED.balance_minor""",
                    e.accountId(), e.currency(), e.amountMinor());
            return Outcome.APPLIED;
        });
    }

    public enum Outcome { APPLIED, DUPLICATE }

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
