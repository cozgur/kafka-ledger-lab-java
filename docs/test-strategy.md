# Test strategy

Scope: the event flow `payments` (KafkaTemplate) -> Kafka -> `ledger` (Spring Kafka listener container) -> PostgreSQL.

## Oracle

For every account: **balance = sum of its payments, each applied exactly once, in sequence order.**
Every test is a way this can be violated, plus a check that it isn't.

## Layers

| Layer | What | Runs |
|---|---|---|
| Compile | `compileJava compileTestJava` | CI job `compile` |
| Event-streaming integration | Real Kafka (apache/kafka 4.1.1, KRaft), Schema Registry, PostgreSQL via Testcontainers; test classes in parallel | CI job `event-streaming`, `./gradlew test` |
| Performance budget | Lag burst, alone after the main suite | `./gradlew perfTest` |
| Stability | Whole suite 20x with `--rerun` | `scripts/flaky-check.sh 20` |

## Risk -> test map

| Risk | Test | How it is proven |
|---|---|---|
| Events of one account applied out of order | `OrderingTest` | 5 accounts x 20 events over 3 partitions; per-account apply order 1..20; nothing parked, so the ledger's reordering can't mask a wrong key. Mutation key=paymentId fails it. |
| Same payment applied twice (resend, redelivery) | `IdempotencyTest` | Duplicate publish; failure after the DB write (in-process redelivery by the error handler); asserts the redelivery happened. Red first (200 instead of 100). |
| paymentId reused with different content (applied or parked) | `IdempotencyTest` | `payment-id-conflict` in the DLT, never applied. Mutation: removing the parked identity check fails it. |
| A new payment claims a taken sequence slot | `IdempotencyTest`, `OutOfOrderTest` | `sequence-conflict` in the DLT, never silently dropped. |
| Late / out-of-order events | `OutOfOrderTest` | 1,3,2 and 3,1,2 applied as 1,2,3; gap parked until filled. Red first. |
| Offset committed before the DB write (loss window) | `OffsetDurabilityTest` | Listener gated before and after the DB write; broker offset must not move in either window. Mutation commit-first fails it (the restart test stays green). |
| Graceful restart loses or repeats work | `RestartTest` | Stop at message 51 with all threads gated, produce while down, restart: 250/250 once, order kept, redeliveries bounded. |
| Poison message: blocked partition or silent loss | `DeadLetterTest` | Invalid payload (wrong types, JSON null/number/string/array, bad JSON, non-EUR, amount out of bound) -> DLT at once; processing error -> 3 attempts -> DLT; transient error recovers; the next payment always flows. Red first: Spring's default handler skipped the record silently. |
| Ledger can't be rebuilt, or is rebuilt wrongly | `ReplayTest` | Wipe + `Admin.alterConsumerGroupOffsets` to earliest + replay -> identical snapshot; after simulated retention the ledger parks instead of guessing. |
| Broker outage loses or duplicates payments | `BrokerOutageTest` | Own broker frozen 3s while producing with forced retries: sends block, then all complete; exactly 60 records in the log; ledger right. Mutation enable.idempotence=false: 90 records. |
| Consumer falls behind on a spike | `LagBudgetTest` (perf) | 5,000-event burst drains to lag 0 within 5s (admin API). Red first: 18.9s. |
| Schema change the registry policy should block | `SchemaCompatibilityTest` | FULL_TRANSITIVE: compatible v2 accepted; 3 breaking changes rejected (409); BACKWARD alone would accept a removal. **Policy only:** the services send JSON. |
| Stream and ledger silently diverge | `ConsistencyTest` | Auditor re-derives balances from the topic without ledger code; stream == ledger per account, ledger == sum of applied, counts. |
| Spring Boot wiring broken | `LedgerApplicationTest` | The Boot context starts, consumes its configured topic, updates the ledger. |

## Three kinds of "crash"

| Kind | Tested by |
|---|---|
| Listener exception (error handler seeks back, same process) | `IdempotencyTest`, `DeadLetterTest` |
| Graceful stop (`stop()`, `stopImmediate=true`) | `RestartTest`, `ReplayTest` |
| Process kill (SIGKILL, pod loss) | **not tested**; the commit boundary it relies on is tested by `OffsetDurabilityTest` |

## Rules

- No fixed sleeps: Awaitility polls observable state (consumer lag, delivery counts) with a timeout. Negative checks use a bounded `during` window.
- Each test owns its topic, DLT, consumer group, DB schema and registry subject. The outage test has its own broker.
- A test must be able to fail: red first where the mechanism didn't exist yet, otherwise a mutation check.
- Fault-injection tests assert the injected fault really happened.

## Known issue: ConsistencyTest is flaky under parallel load (open)

2 failures in 55 runs of the main suite (1,893 test executions in total). Symptom: `Expiring N record(s) for <topic>-<p>: 120001 ms has
passed since batch creation`, i.e. the producer got no ack for one partition of a fresh topic for the whole delivery timeout, while the
other test classes were starting. Not reproduced in 15 runs with client logs. Refuted hypothesis: interference from `BrokerOutageTest`
pausing its own broker (it recurred with that test moved to `isolatedTest`). Not done on purpose: raising timeouts or retrying the test,
which would hide the symptom. Next step: inspect `build/test-logs/kafka-clients.log` at the next occurrence (leader/metadata errors on
the new topic, idempotent producer sequence errors).

## Not covered

Real process kill; rebalance between two consumer instances; DB failure mid-transaction; DLT write failure; Kafka transactions /
read_committed; multi-broker failover, ISR, `min.insync.replicas`; Avro on the wire (the services send JSON); multi-version schema replay;
alerting and repair for parked and dead-lettered events; security; production performance.
