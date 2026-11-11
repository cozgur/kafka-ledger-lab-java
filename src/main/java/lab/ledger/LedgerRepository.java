package lab.ledger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lab.payments.PaymentCaptured;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Ledger persistence. Each instance works inside its own Postgres schema, so tests can run in parallel
 * against one database without seeing each other's rows.
 */
public class LedgerRepository implements AutoCloseable {

    private final HikariDataSource dataSource;
    protected final JdbcTemplate jdbc;

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
        // The whole apply rule as ONE server-side function: one round trip per event instead of ~6-8. A single
        // statement runs atomically, so the identity row, balance and last_sequence still change together.
        jdbc.execute("""
                CREATE OR REPLACE FUNCTION apply_payment(
                  p_payment_id text, p_account_id text, p_amount bigint, p_currency text, p_sequence int
                ) RETURNS text LANGUAGE plpgsql AS $fn$
                DECLARE
                  v_last int;
                  v_next record;
                BEGIN
                  -- paymentId is a GLOBAL identity; the account row lock below only serialises one account. Lock the identity
                  -- first (transaction-scoped advisory lock), then the account. Same order in every call -> no deadlock.
                  PERFORM pg_advisory_xact_lock(hashtext(p_payment_id));
                  INSERT INTO balances (account_id, currency, balance_minor) VALUES (p_account_id, p_currency, 0) ON CONFLICT DO NOTHING;
                  SELECT last_sequence INTO v_last FROM balances WHERE account_id = p_account_id FOR UPDATE;
                  -- identity: seen before (applied or parked)? same content = duplicate, different = conflict
                  IF EXISTS (SELECT 1 FROM applied_payments WHERE payment_id = p_payment_id) THEN
                    IF EXISTS (SELECT 1 FROM applied_payments WHERE payment_id = p_payment_id
                               AND account_id = p_account_id AND amount_minor = p_amount AND sequence = p_sequence) THEN
                      RETURN 'DUPLICATE';
                    END IF;
                    RETURN 'CONFLICT';
                  END IF;
                  IF EXISTS (SELECT 1 FROM parked_events WHERE payment_id = p_payment_id) THEN
                    IF EXISTS (SELECT 1 FROM parked_events WHERE payment_id = p_payment_id
                               AND account_id = p_account_id AND amount_minor = p_amount AND sequence = p_sequence) THEN
                      RETURN 'DUPLICATE';
                    END IF;
                    RETURN 'CONFLICT';
                  END IF;
                  -- slot: a new paymentId on a taken sequence (applied or parked) = sequence conflict
                  IF p_sequence <= v_last THEN RETURN 'SEQUENCE_CONFLICT'; END IF;
                  IF p_sequence > v_last + 1 THEN
                    IF EXISTS (SELECT 1 FROM parked_events WHERE account_id = p_account_id AND sequence = p_sequence) THEN
                      RETURN 'SEQUENCE_CONFLICT';
                    END IF;
                    INSERT INTO parked_events (account_id, sequence, payment_id, amount_minor, currency)
                    VALUES (p_account_id, p_sequence, p_payment_id, p_amount, p_currency);
                    RETURN 'PARKED';
                  END IF;
                  INSERT INTO applied_payments (payment_id, account_id, amount_minor, sequence, was_parked)
                  VALUES (p_payment_id, p_account_id, p_amount, p_sequence, false);
                  UPDATE balances SET balance_minor = balance_minor + p_amount, last_sequence = p_sequence WHERE account_id = p_account_id;
                  v_last := p_sequence;
                  LOOP -- drain parked successors in order
                    DELETE FROM parked_events WHERE account_id = p_account_id AND sequence = v_last + 1
                    RETURNING payment_id, amount_minor INTO v_next;
                    EXIT WHEN NOT FOUND;
                    v_last := v_last + 1;
                    INSERT INTO applied_payments (payment_id, account_id, amount_minor, sequence, was_parked)
                    VALUES (v_next.payment_id, p_account_id, v_next.amount_minor, v_last, true);
                    UPDATE balances SET balance_minor = balance_minor + v_next.amount_minor, last_sequence = v_last WHERE account_id = p_account_id;
                  END LOOP;
                  RETURN 'APPLIED';
                END
                $fn$""");
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
     * Apply one event idempotently and in per-account sequence order (rule implemented in apply_payment above):
     * 1. lock the account row (concurrent applies for one account are serialised)
     * 2. paymentId seen (applied or parked) -> same content DUPLICATE, different content CONFLICT
     * 3. new paymentId on a taken slot (sequence <= last, or already parked) -> SEQUENCE_CONFLICT
     * 4. sequence >  last_sequence + 1 -> PARKED (gap: wait for the missing event)
     * 5. sequence == last_sequence + 1 -> APPLIED, then drain parked successors in order
     */
    public Outcome applyPayment(PaymentCaptured e) {
        return Outcome.valueOf(jdbc.queryForObject("SELECT apply_payment(?, ?, ?, ?, ?)", String.class,
                e.paymentId(), e.accountId(), e.amountMinor(), e.currency(), e.sequence()));
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
