# kafka-ledger-lab-java

[![ci](https://github.com/cozgur/kafka-ledger-lab-java/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cozgur/kafka-ledger-lab-java/actions/workflows/ci.yml)

A small, fintech-flavoured lab for **testing Kafka-based event flows in Java / Spring Kafka**: a `payments` producer publishes
`PaymentCaptured` events, a `ledger` Spring Kafka consumer applies them to account balances in PostgreSQL, and a JUnit 5 suite proves the
delivery guarantees between them against a real broker.

> **Honest note:** this is a personal learning lab built in November 2026, not production experience, and it has never handled real money.
> It is a Java/Spring Kafka port of my earlier TypeScript lab, written with an AI assistant (Claude; see the commit trailers). The commit
> history follows the same path as the TypeScript lab: each guarantee was first shown failing (red) against a naive or default
> implementation, then fixed. Where Java/Spring behaved differently, the difference was measured and written down in
> [java-vs-ts-findings.md](docs/java-vs-ts-findings.md). What is *not* covered is listed below.

## Event streaming

```
payments (KafkaTemplate) --PaymentCaptured (JSON), key=accountId--> Kafka topic --> ledger (Spring Kafka listener) --> PostgreSQL
                                                                                         |
                                                                                         +--> <topic>.dlt (DeadLetterPublishingRecoverer)

Tests only: Schema Registry (Avro schemas in schemas/, FULL_TRANSITIVE) checks the compatibility policy.
It is not on the message path: the services send JSON.
```

**Spring Boot scope:** the listener container, error handler and producer are built programmatically from Spring Kafka components
(`ConcurrentMessageListenerContainer`, `DefaultErrorHandler`, `DeadLetterPublishingRecoverer`, `KafkaTemplate`) so that every test can own
its topic, consumer group, DLT and DB schema and test classes can run in parallel. `LedgerApplication` wires the same components as a Boot
service and `LedgerApplicationTest` checks that wiring, but Boot's own Kafka auto-configuration, `@KafkaListener` and `application.yml`
defaults are **not** tested here.

**Stack:** Java 21, Spring Boot 4.1, Spring Kafka 4.1, Apache Kafka 4.1.1 (KRaft, `apache/kafka`), Confluent Schema Registry 8.1.0,
PostgreSQL 17, Testcontainers 2, JUnit 5, Awaitility, Gradle, GitHub Actions.

**What the tests prove** ([test strategy](docs/test-strategy.md) maps each one to its risk):

| Test class | Guarantee |
|---|---|
| `OrderingTest` | Events of one account are applied in order (Kafka orders per partition only; key = accountId); a wrong key cannot be masked |
| `IdempotencyTest` | A resent or redelivered payment changes the balance once; a paymentId reused with different content, or a new payment on a taken sequence, is dead-lettered (applied or parked) |
| `OutOfOrderTest` | Out-of-order and late events are applied in sequence (park until the gap fills) |
| `OffsetDurabilityTest` | The offset becomes durable only after the DB write and the listener completed (the at-least-once loss window) |
| `RestartTest` | A graceful stop mid-stream and restart loses nothing and applies nothing twice (a real process kill is not tested) |
| `DeadLetterTest` | Poison messages (bad fields, JSON null/array/number, bad JSON, non-EUR, amount out of bound) go to the DLT with diagnostics; processing errors after 3 attempts; the partition keeps flowing |
| `ReplayTest` | Reset the group offset and replay: identical ledger; missing history is parked, not guessed |
| `BrokerOutageTest` | Broker frozen while producing: no loss, no duplicate records (idempotent producer, retries forced), ledger correct |
| `LagBudgetTest` | A 5,000-event burst drains to lag 0 within a 5s budget (lag via the Admin API), measured alone |
| `SchemaCompatibilityTest` | Registry policy: compatible v2 accepted, breaking changes rejected (policy only; services send JSON) |
| `ConsistencyTest` | An independent auditor reconciles the topic against the ledger without using ledger code |
| `LedgerApplicationTest` | The Spring Boot application context consumes and applies payments |

### Run it

Requirements: Docker and Java 21.

```bash
./gradlew check            # everything: test (classes in parallel) -> isolatedTest (broker outage) -> perfTest (lag), each alone
scripts/flaky-check.sh 20  # whole suite 20x, counting only tests that really ran; report in build/flaky/summary.md
```

### Results

Measured locally on 2026-11-11 (Apple Silicon laptop, Docker Desktop, images cached):

- **40 tests** in 14 classes. `./gradlew check` runs `test` (38 tests, classes in parallel, ~30s), then `isolatedTest` (broker outage) and
  `perfTest` (lag budget), each alone.
- **Flakiness: 20 consecutive runs, 800 test executions, 0 failures.** Results are cleared before every run and a run only counts tests that
  really executed; all three tasks ran in all 20 runs.
- **Lag burst:** 5,000 events drained in 1.0-2.0s across the 20 runs (budget 5s). It started at 18.9s; see the commit history.
- **A flaky test was found and fixed on the way.** Producers sometimes got no ack for 120s right after a topic was created. The client log
  showed `NOT_LEADER_OR_FOLLOWER`, then the idempotent producer's retries failing with `OUT_OF_ORDER_SEQUENCE_NUMBER` for the whole delivery
  timeout. Tests now wait until every partition leader answers before producing; the error is absent from the client logs of all 20 runs.
  Earlier flakiness counts in the history were inflated by a bug in my own script (it credited runs with stale results) and are withdrawn.
- **CI (GitHub Actions, ubuntu-latest):** [run 43105506842](https://github.com/cozgur/kafka-ledger-lab-java/actions/runs/43105506842):
  `compile` and `event-streaming` in parallel, **40 tests, 0 failures**; lag burst drained in 1,723 ms on the runner. Evidence (JUnit XML,
  lag.json, Kafka client logs) is attached to each run as an artifact.

### Read more

- [Test strategy](docs/test-strategy.md): risk -> test map, the three kinds of "crash", rules, what is not covered
- [Java vs. TypeScript findings](docs/java-vs-ts-findings.md): what behaved differently in Spring Kafka, and how each was found
- [ADR 0001](docs/adr/0001-event-streaming-test-layer.md): design decisions and scope

### Out of scope

Real process kill (SIGKILL/pod loss), rebalance between two consumer instances, DB failure mid-transaction, DLT write failure,
exactly-once across Kafka and Postgres / Kafka transactions, multi-broker failover, Avro on the wire, multi-version schema replay,
alerting/repair for parked and dead-lettered events, security, production performance testing. The ledger is EUR-only.
