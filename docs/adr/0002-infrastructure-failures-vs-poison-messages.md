# ADR 0002: Infrastructure failures are not poison messages

- Status: accepted
- Date: 2026-11-12

## Context

The first error policy (ADR 0001) gave every processing failure 3 attempts and then sent the record to the dead-letter topic.
`DbOutageTest` showed what that means during a database outage: every payment fails, so valid payments land in the DLT and a short
outage becomes a manual repair job (red: 1 valid payment dead-lettered).

## Decision

Classify the failure before choosing the back-off (`LedgerConsumer.isInfrastructureFailure`):

| Failure | Examples | Policy |
|---|---|---|
| Invalid payload | bad JSON / types, non-EUR, amount out of bound | DLT at once (retrying cannot fix it) |
| Conflict | paymentId reused with other content, sequence slot taken | DLT at once, for a human |
| Infrastructure | `TransientDataAccessException`, `DataAccessResourceFailureException`, `SQLTransient`/`Recoverable` exceptions, SQLState `08*` / `57P*` | retry without limit, exponential back-off up to 5 s; the partition waits |
| Other processing error | anything else | 3 attempts, then DLT |

The DLT write itself must succeed before the source offset is committed (`setFailIfSendResultIsError(true)`, made explicit);
if the DLT is unavailable the partition waits instead of losing the record (`DltWriteFailureTest`).

## Consequences

- During an outage the affected partitions stop and lag grows. That is the intended signal: lag and error-rate alerts should fire,
  nobody should have to replay payments from the DLT afterwards.
- A misclassified permanent error (e.g. a schema bug that surfaces as a connection error) would block a partition indefinitely.
  Mitigation for a real system: alert on "partition not progressing for N minutes" and keep the classification list small.
- `max.poll.interval.ms` (5 min default) is far above the 5 s maximum back-off, so waiting does not trigger a rebalance.
