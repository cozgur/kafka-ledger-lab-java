# ADR 0001: Event-streaming test layer for the payments -> ledger flow (Java / Spring Kafka)

- Status: accepted
- Date: 2026-11-11
- Context: personal learning lab, not a production system. Java/Spring Kafka port of an earlier TypeScript lab.

## Decision

1. **Real infrastructure, no mocks.** Apache Kafka 4.1.1 in KRaft mode (`apache/kafka`), Confluent Schema Registry 8.1.0 and PostgreSQL 17 via
   Testcontainers, started once per test JVM. Each test owns its topic, DLT, consumer group, DB schema and registry subject; test classes run in
   parallel. The broker-outage test gets its own broker.
2. **Stack close to a typical Java fintech service:** Java 21, Spring Boot 4.1, Spring Kafka 4.1 (listener container, `KafkaTemplate`,
   `DefaultErrorHandler`, `DeadLetterPublishingRecoverer`), JUnit 5, Awaitility, Gradle.
3. **At-least-once + idempotent consumer.** `AckMode.BATCH`: offsets of a poll are committed after the listener processed them, and the DB write
   happens inside the listener. `stopImmediate=true`. Idempotent producer, `acks=all`.
4. **Ledger rule in one Postgres function** (`apply_payment`): row lock per account; paymentId identity (same content = duplicate, different =
   conflict) and sequence slots (new payment on a taken slot = conflict) for applied and parked payments; gap -> park; next -> apply and drain.
5. **Explicit error policy** (infrastructure failures refined in [ADR 0002](0002-infrastructure-failures-vs-poison-messages.md)): invalid payloads and conflicts are not retried; processing errors get 3 attempts; then the DLT with the original bytes
   and `dlt.*` headers. Spring's default (10 attempts, then skip) is rejected because it silently loses the payment.
6. **Contract:** Avro schemas in `schemas/`, subject compatibility FULL_TRANSITIVE (policy tests; the services send JSON).
7. **Determinism:** Awaitility polling instead of sleeping for results (negative checks use bounded `during` windows; the only fixed interval is the lag sampling interval); deterministic gates for fault injection; every fault test asserts the fault happened; the lag budget
   runs alone; the suite is run 20x with `--rerun` before claiming stability.

## Guarantees tested

See [test-strategy.md](../test-strategy.md) for the risk -> test map and [java-vs-ts-findings.md](../java-vs-ts-findings.md) for what behaved
differently from the TypeScript version.

## Out of scope

Exactly-once across Kafka and Postgres; Kafka transactions; multi-broker failover, ISR, leader election; an instance dying during a
rebalance (a graceful rebalance and a SIGKILL of a single instance are tested); Avro on the wire; multi-version schema replay; alerting/repair for parked and
dead-lettered events; security; production performance.

## Consequences

- A suite run takes about a minute on a laptop (containers + Spring); slower than unit tests, but every check runs against real broker and DB behaviour.
- Business rules live in SQL (`apply_payment`), covered by integration tests rather than unit tests.
- The lag budget is a regression guard on a single broker, not a production SLO.
