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
| Partitions move between instances (scale-out / scale-in) | `RebalanceTest` | A second instance joins mid-stream (wait until the 6 partitions are split between both before producing more), then the first leaves: all 300 applied once, order kept, deliveries bounded. Graceful leave only. |
| A crash of the process in the loss window | `ProcessKillTest` (isolated) | Ledger service as its own JVM; every listener thread parks after the DB write once 120 records were processed; SIGKILL while applied-in-DB > committed offsets; restart: 300/300 once, order kept, nothing dead-lettered. Mutation: removing the applied-identity check sends 122 redeliveries to the DLT. |
| A database outage dead-letters valid payments | `DbOutageTest` (isolated) | Own Postgres behind Toxiproxy; connection cut while payments arrive, more than the 3-attempt poison budget passes, connection restored: 0 DLT, all applied in order. Red first (1 valid payment dead-lettered). See ADR 0002. |
| Partial state after a failure inside the ledger transaction | `TransactionRollbackTest` | A trigger fails the balance update once, after the dedupe row was inserted: between the failed attempt and the retry there is no dedupe row and no balance; the retry applies once. Mutation: swallowing the failure inside `apply_payment` fails it. |
| A poison message is lost when the DLT is unavailable | `DltWriteFailureTest` | The test's DLT rejects every write: the source offset stays put, the DLT stays empty; after recovery the record is dead-lettered and the payment behind it applied. Mutation: `failIfSendResultIsError(false)` fails it. |
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
| Graceful stop (`stop()`, `stopImmediate=true`) | `RestartTest`, `ReplayTest`, `RebalanceTest` (an instance leaves the group) |
| Process kill (SIGKILL) | `ProcessKillTest`: the service JVM is killed inside the loss window and restarted |

## Rules

- No sleep is used to wait for a result: tests poll observable state (consumer lag, delivery counts) with timeouts and use synchronization gates for fault injection. Negative checks ("this has NOT happened yet") use a bounded Awaitility `during` window. The only fixed interval is the 100 ms sampling interval of the lag measurement.
- Each test owns its topic, DLT, consumer group, DB schema and registry subject. The outage test has its own broker.
- A test must be able to fail: red first where the mechanism didn't exist yet, otherwise a mutation check.
- Fault-injection tests assert the injected fault really happened.

## Resolved flake: no producer ack for 120s on a fresh topic

Symptom (seen in `ConsistencyTest` and `RestartTest`, about 1 in 30 runs of the main suite): `Expiring N record(s) for <topic>-<p>:
120001 ms has passed since batch creation`. Root cause, from the Kafka client log: the first produce right after `createTopics()` got
`NOT_LEADER_OR_FOLLOWER` (the broker had not loaded partition leadership yet), and every retry of the idempotent producer then failed with
`OUT_OF_ORDER_SEQUENCE_NUMBER` (123 times) until the delivery timeout. Fix: `Lab.awaitLeaders` waits until each partition leader answers
`ListOffsets` before anything is produced. Refuted on the way: interference from `BrokerOutageTest`. Not done on purpose: raising timeouts
or retrying tests. After the fix: 20 runs, 800 executions, 0 failures, no `OUT_OF_ORDER_SEQUENCE_NUMBER` in any client log.

## Not covered

An instance dying in the middle of a rebalance; Kafka transactions /
read_committed; multi-broker failover, ISR, `min.insync.replicas`; Avro on the wire (the services send JSON); multi-version schema replay;
alerting and repair for parked and dead-lettered events; security; production performance.
