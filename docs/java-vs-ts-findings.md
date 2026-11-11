# What changed when the lab moved from TypeScript (librdkafka) to Java (Spring Kafka)

The TypeScript lab used `@confluentinc/kafka-javascript` (librdkafka). This Java version uses the Apache Kafka Java client through
Spring Kafka. The guarantees under test are the same; several behaviours were not, and each one below was found by a test, not assumed.

| # | Topic | TypeScript lab | Java / Spring Kafka (this repo) | Evidence |
|---|---|---|---|---|
| 1 | Poison message, default behaviour | `eachMessage` throws -> the client seeks back forever: **partition blocked** | `DefaultErrorHandler` default: 10 attempts, then log and **skip**: partition flows, payment **silently lost** | `DeadLetterTest`, red commit |
| 2 | Graceful stop | Stopped promptly | `stop()` first processes every already-fetched record (`stopImmediate=false`): a "mid-stream" stop drained 200/200; fixed with `setStopImmediate(true)` | `RestartTest`, red twice |
| 3 | Lag bottleneck | DB round trips (and per-message commits piling up so broker lag overstated progress) | One **synchronous** commit per record (`AckMode.RECORD`): 18.9s; `AckMode.BATCH` 3.7s; one DB round trip 1.4s. Broker lag tracked real progress | `LagBudgetTest` |
| 4 | Offset reset for replay | KafkaJS-compatible admin had none: shell out to `kafka-consumer-groups` | `Admin.alterConsumerGroupOffsets` (group must be empty) | `ReplayTest` |
| 5 | JSON `null` payload | `null.paymentId` threw a TypeError -> endless retry, partition blocked (review finding) | Jackson `JsonNode.get` on a non-object returns null; the validator rejects it as invalid -> DLT | `DeadLetterTest` (green at once) |
| 6 | Early-commit mutation | Caught only by the dedicated offset-durability test | Also caught by the in-process redelivery test; the graceful restart test misses it in both | mutation runs |
| 7 | Kafka image in Testcontainers | Node module supported only `cp-kafka` | Java module supports `apache/kafka` natively | `Infra` |
| 8 | Producer idempotence | Not exercised | Broker frozen with forced retries: idempotence on = 60 records, off = **90** (30 duplicates). With the default 30s request timeout no retry happened, so the check proved nothing | `BrokerOutageTest` |
| 9 | Test-infra traps | Lag test flaky when run next to 11 parallel files | Class-init deadlock: a lambda in `Infra`'s static block runs as a static method of `Infra` on another thread; Gradle reports an unchanged test task UP-TO-DATE and runs nothing (`--rerun`) | commit history |

| 10 | Producing right after creating a topic | Not observed | Idempotent producer stuck for 120s: `NOT_LEADER_OR_FOLLOWER`, then `OUT_OF_ORDER_SEQUENCE_NUMBER` on every retry; fixed by waiting for partition leaders | client log, test strategy |

What I would re-verify on any other Java/Spring consumer: the error handler and its back-off (does it skip, block, or dead-letter?),
`AckMode` and when offsets become durable, `stopImmediate` / shutdown behaviour during deploys, `max.poll.interval.ms` against the
slowest handler path, and producer `enable.idempotence` / `acks` / retry timeouts.
