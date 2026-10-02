# Architecture

The current runtime is composed with a named Uber Fx module (`backend-api`).
Construction, startup and shutdown are lifecycle-managed so HTTP, SQS workers,
the outbox publisher, pending-reference worker, migrations and PostgreSQL release
are part of one application composition.

The current runtime is composed with Uber Fx:

```text
HTTP -> application -> ports -> PostgreSQL infrastructure
                                      |
                                      +-> transactional outbox -> Publisher -> event SQS FIFO
SQS command FIFO -> Consumer -> Inbox + application/domain transaction
```

The domain packages do not depend on HTTP, PostgreSQL, AWS SDK, or LocalStack.
Financial processing remains in the existing application and PostgreSQL
repository flow. Wallet locking, ledger writes, wager transaction state, and
outbox events remain in the same PostgreSQL transaction.

The transactional outbox is the bridge between the database transaction and the
event SQS FIFO queue. When a wallet or wager transaction is processed, the financial
state and the corresponding outbox record are inserted in the same SQL
transaction. This keeps the money movement durable even when publishing happens
outside the financial transaction.

HTTP wallet and wagering routes are protected by an OIDC middleware. The
middleware validates Bearer tokens using the configured issuer and JWKS,
including expiration and audience when configured, then places an identity in
the request context. Wagering derives `provider_id` exclusively from the
configured provider claim, so a body field cannot switch providers: an absent
body `providerId` is accepted, a matching one is accepted and a different one is
refused with `403 provider_mismatch` before the use case runs. Authorization is
role based on Keycloak realm roles: `wallet-internal` (only the
`backend-internal` service account) for wallet creation, reads, ledger and
reconciliation; `wagering-provider` (provider users) for submitting operations
and provider lookups. Provider isolation is also enforced below HTTP:
idempotency keys, external IDs and reference resolution are all scoped by
`provider_id` in the unique indexes and queries, so replays and reversals never
cross providers. The application domain and `WageringService` do not depend on
the OIDC library.

### Health checks

Both endpoints are public and accept only `GET`.

- `/health/live` answers `200` while the process is running. It does not check
  dependencies, so an outage of PostgreSQL or SQS does not make the orchestrator
  restart instances that will recover by themselves.
- `/health/ready` checks PostgreSQL (`Ping` on the pool) and SQS (resolving the
  URLs of the command queue, the DLQ and the event queue, so a missing queue also
  fails). It returns `200` with `{"status":"ready"}`, or `503` with
  `{"status":"not_ready","checks":{"postgres":"ok|error","sqs":"ok|error"}}`.

The two checks run concurrently, each with its own 3-second deadline, so a
hanging dependency cannot make the other one look unavailable and the endpoint
answers within about 3 seconds. Each failed check is logged as
`readiness_check_failed` with `dependency` (`postgres`/`sqs`) and `reason`
(`timeout`/`unavailable`), and it increments `readiness_check_failures_total`.
The raw error is not logged because it may contain hosts or database user names.

During a temporary outage the instances keep running. A request that fails
because of a temporary database condition (unreachable server, dropped
connection, timeout, deadlock, serialization failure or another retryable
SQLSTATE, see `database.IsTransient`) answers `503`
`{"error":"service_unavailable"}` with `Retry-After: 1`. Nothing was committed,
so the client can retry with the same `Idempotency-Key`. Any other unexpected
error stays `500` `{"error":"internal_error"}`. Both cases are logged as
`request_failed` with the route, and 503s also increment
`http_dependency_unavailable_total`. HTTP writes do not depend on SQS: events go
through the outbox, so `POST /wagering/transactions` keeps working while SQS is
down and the events are published after it returns. Meanwhile the SQS
consumer, the outbox publisher and the reference worker log the failure and
retry on their next cycle. This was verified by stopping and restarting the
PostgreSQL and LocalStack containers under three instances: readiness switched
to `503` for the affected dependency only, liveness stayed `200`, and HTTP, SQS
consumption and outbox publication resumed without restarting the application.

The publisher is implemented as a separate application worker. It claims a small
batch of pending outbox rows using PostgreSQL `FOR UPDATE SKIP LOCKED`, checks
`next_attempt_at`, and releases a lease via `locked_at` so multiple application
processes can work concurrently without claiming the same event twice.
Each process has a unique `locked_by` owner token. Mark-published and reschedule
updates require that token, preventing a publisher whose lease expired from
overwriting a row that another process reclaimed.

Retries use exponential backoff (`OutboxRetryBaseDelay` multiplied by 2^n), and
abandoned records are recovered when the lease has expired. The worker leaves the
outbox row pending until the event is successfully sent to SQS, then marks it as
`PUBLISHED`. It does not assume exactly-once delivery: sending can be repeated,
so the stable `event_id` is reused on retries and set as the FIFO
`MessageDeduplicationId`. The latest publication error is retained in
`outbox_events.last_error`.

For FIFO ordering, the SQS message group is the aggregate ID (`aggregate_id`),
which preserves ordering within a wallet or wager flow while allowing unrelated
aggregates to be processed in parallel. The event body is sent as a typed
envelope:

```json
{
  "eventId": "...",
  "eventType": "WalletBalanceChanged",
  "aggregateId": "...",
  "correlationId": "...",
  "causationId": "...",
  "occurredAt": "2026-09-30T12:00:00Z",
  "version": 1,
  "data": { ... }
}
```

The `eventId` is stable across publication retries, `eventType` identifies the
concrete event, and `occurredAt` represents when the event occurred rather than
when it was delivered to SQS. The `data` field contains the immutable snapshot
persisted by the outbox.

### Integration event contract

Each event has a concrete Go payload type in `internal/messaging/events.go`. Only
the event constructors (`NewWagerTransactionProcessed`, `NewWagerTransactionRejected`,
`NewWalletBalanceChanged`, `NewWagerTransactionPendingReference`) define
`eventType`, `version` and the aggregate. The payload is a sealed interface, so an
event cannot carry arbitrary data. Before sending, the publisher validates the
envelope: the event type must be known and its version must match the
registered contract, IDs must be present, `occurredAt` must be UTC and `data`
must be a non-null JSON object.

Envelope fields:

| Field | Meaning |
| --- | --- |
| `eventId` | UUID generated when the outbox row is written; reused on every republication and sent as `MessageDeduplicationId`. |
| `eventType` / `version` | Set by the constructor. All events are currently at version `1`. |
| `aggregateId` | `WalletBalanceChanged`: the wallet ID. The three `WagerTransaction*` events: the internal transaction ID, including `OPENING`. Also the FIFO `MessageGroupId`. |
| `correlationId` | The internal transaction ID. All events from the same operation share it, whether it arrived via HTTP or SQS and whether it finished synchronously or in the reference worker. The HTTP `Correlation-ID` and the SQS `messageId` are request/transport identifiers. They appear in the logs together with `transactionId`, which links them to the events. |
| `causationId` | Optional and currently omitted: no event is caused by another integration event. |
| `occurredAt` | UTC RFC 3339 timestamp of the commit that produced the event. |

Payloads (`data`). All money values are `{"amount":"25.00","currency":"BRL"}`
with decimal strings, never JSON numbers.

| Event | Trigger | Fields |
| --- | --- | --- |
| `WagerTransactionProcessed` | Successful operation, including `LOSS` and the internal `OPENING` | `transactionId`, `walletId`, `playerId`, `kind`, `status`, `money`, `result.balance`, `result.version`; for external operations also `providerId`, `externalTransactionId`, `roundId`, `gameId` and, for reversals, `referenceExternalTransactionId` |
| `WagerTransactionRejected` | Definitive business rejection, including reference expiry | same identification fields as above, plus `failureCode`; for reference expiry also `referenceAttempts`; `result` holds the wallet balance and version observed at rejection |
| `WalletBalanceChanged` | Effective balance change (never for `LOSS` or rejections) | `walletId`, `transactionId`, `direction` (`DEBIT`/`CREDIT`), `money`, `balanceBefore`, `balanceAfter`, `walletVersion`, `previousVersion` |
| `WagerTransactionPendingReference` | Reversal waiting for its reference (emitted once, when the wait is registered; retries only advance the persisted backoff) | `transactionId`, `walletId`, `providerId`, `kind`, `status`, `failureCode`, `referenceExternalTransactionId`, `referenceAttempts`, `nextAttemptAt` |

`OPENING` is internal: its `WagerTransactionProcessed` omits the external fields
(`providerId`, `externalTransactionId`, `roundId`, `gameId`) instead of sending
them empty.

Routing and consumption: every event goes to the `wager-events.fifo` queue,
which is separate from the command queue. Consumers should route by `eventType`,
deduplicate by `eventId` (delivery is at-least-once, and SQS deduplication only
covers a 5-minute window), and rely on order only within one `aggregateId`.
For example, a wallet's `WalletBalanceChanged` events arrive in `walletVersion`
order, but there is no ordering guarantee between a transaction's
`WagerTransactionProcessed` and the matching `WalletBalanceChanged`.

Snapshot immutability is enforced by the database. Migration `000004` adds a
trigger that rejects any `UPDATE` to `event_id`, aggregate, `event_type`,
`correlation_id`, `causation_id`, `occurred_at`, `version`, `payload` or
`created_at`. Only the delivery state (`status`, `attempts`, `next_attempt_at`,
lease, `published_at` and `last_error`) can change.

The command SQS FIFO queue receives `WagerTransactionRequested` messages. The
consumer long-polls the queue, groups messages by `MessageGroupId`, records
`consumer_name + message_id + payload_hash` in PostgreSQL, invokes the same
financial application use case as HTTP, marks the Inbox row complete, and
commits Inbox, domain state, ledger, and Outbox changes in one PostgreSQL
transaction. It deletes the SQS message only after that commit. The Outbox
Publisher sends `WagerTransactionProcessed`, `WagerTransactionRejected`,
`WagerTransactionPendingReference`, and `WalletBalanceChanged` to the separate
event SQS FIFO queue. These events are never interpreted as input commands.
Failed processing leaves command messages available for redelivery and the
existing SQS redrive policy remains responsible for the DLQ.

Consumer delivery is at-least-once. The SQS adapter reads
`ApproximateReceiveCount` and exposes it with the message, so retry state is
not kept only in application memory and survives restarts and multiple
instances. A processing failure leaves the message undeleted and schedules its
next delivery with `ChangeMessageVisibility`. The delay is exponential,
starting at 1 second and capped at 30 seconds by the application defaults.
There is no worker sleep while waiting for a retry.

The queue redrive policy, not the application, decides when a message reaches
the DLQ (`SQS_MAX_RECEIVE_COUNT`, default 5 in Compose). Invalid JSON, invalid
envelopes, and invalid commands are poison messages: they cannot produce a
financial effect and remain undeleted so SQS can redrive them. Business
rejections are different: the financial transaction persists the rejected
result, then the committed message is deleted. This prevents permanent
business decisions from becoming infinite retries.

Retry and DLQ-eligibility counters are observations only. In particular,
`sqs_messages_dlq_eligible_total` does not delete or quarantine a message and
does not replace the queue's redrive decision.

A command that reuses an idempotency key with a different payload (or an
external ID with another key) is a permanent input error, not a business
rejection: it has no financial effect, stays undeleted, and reaches the DLQ
once the receive limit is exhausted. The same happens to valid commands if
PostgreSQL stays unavailable through all their attempts (5 receives by default,
retried 1 to 30 s apart). Such messages are not lost: they wait in the DLQ and
can be moved back to the command queue once the cause is fixed, and the Inbox
and idempotency keys make that replay safe.

On SIGTERM the consumer uses two contexts. The polling context is cancelled at
once, so no new message is received. The batch already received (at most 10
messages) is completed with a separate work context, so its transactions commit
and its messages are deleted before the process exits. Only if the Fx stop
deadline expires is that work context cancelled: the open transactions roll
back and every message that did not complete has its visibility set to 0, so
another replica receives it immediately instead of after the visibility
timeout. A replica that dies without running this (SIGKILL, crash) leaves its
received messages invisible until the visibility timeout (30 s). They are then
redelivered: if the transaction had committed, the Inbox stops the duplicate;
otherwise the survivor applies it.

A related at-least-once effect: when a replica stops in the middle of a 20 s
long poll, LocalStack can still hand a newly arriving message to that
closed request. The message then becomes visible again only after the
visibility timeout, so a command sent right after replicas are replaced can take
about 30 s to be processed. It is still processed exactly once.

The application also runs a pending-reference worker for
`PENDING_REFERENCE`. It polls due rows from `wager_transactions`, claims one
row with `FOR UPDATE SKIP LOCKED`, and keeps that lock while the existing
reference retry transaction completes. The persisted
`reference_next_attempt_at` and `reference_attempts` values continue to control
exponential backoff and the existing attempt limit. Because the pending state
is stored in PostgreSQL, a new application instance can recover it after a
restart, and multiple instances can process different pending rows safely.

### Pending references

A `REFUND` or `ROLLBACK` whose reference does not exist yet is committed as
`PENDING_REFERENCE` with `failureCode` `reference_pending`, without a ledger
entry or balance change. HTTP answers `202 Accepted` without a balance, and the
`WagerTransactionPendingReference` event is emitted once in the same commit.

| Situation found by a retry | Result |
| --- | --- |
| Reference missing | Stays pending, increments `reference_attempts` and schedules the next attempt |
| Reference is `PENDING` or `PENDING_REFERENCE` (e.g. a `ROLLBACK` of a `REFUND` that is still waiting) | Stays pending, same as a missing reference |
| Reference `PROCESSED` | The reversal is validated and applied like a synchronous one: `PROCESSED`, or `REJECTED` with `reference_incompatible`, `reversal_already_processed` or `reversal_insufficient_funds` |
| Reference `REJECTED` or `FAILED` | Rejected immediately with `reference_incompatible`; it does not wait |
| Attempts exhausted | `REJECTED` with `reference_not_found` and a `WagerTransactionRejected` event |

Retry policy: the first attempt runs 1 minute after registration and each
following delay doubles (1, 2, 4, 8, 16 minutes). After 5 unsuccessful retries
(`maxReferenceAttempts`), roughly 31 minutes after registration, the operation is
rejected. The rejection stores the wallet balance and version read at that
moment, so idempotent replays return them, as they do for other rejections.
`reference_pending` is a status, not a rejection: the provider should poll
`GET /wagering/transactions/{id}` or consume the events. `reference_not_found`
is final for that operation; the provider must send a new operation with another
`externalTransactionId`.

The worker claims rows in `reference_next_attempt_at` order, one per
transaction, and takes the same locks as the synchronous path: first the
reference row, then the wallet. This avoids lock-order inversion between the
worker and HTTP/SQS. A transaction that fails because of infrastructure (e.g.
PostgreSQL unavailable) is rolled back without consuming an attempt, and the
row is picked up again on the next poll (every 1 second).
Metrics: `reference_worker_processed_total` and `reference_worker_failures_total`.

The Inbox is the durable protection against duplicate delivery and does not
rely on SQS deduplication alone.

Command envelopes carry a durable `messageId`, `type`, `occurredAt` and `data`.
The command `messageId` is used as the Inbox key, while the nested
`idempotencyKey` remains the identity of the financial operation. A message is
deleted from SQS only after the Inbox, financial state and Outbox changes have
committed.

Wallet ledger reads use keyset pagination ordered by `(created_at, id)` and do
not use SQL offsets.

### Reconciliation

`POST /wallets/{walletId}/reconciliation` (role `wallet-internal`) rebuilds the
balance from the ledger (credits minus debits, including the `OPENING` credit),
compares it with the stored balance and returns `storedBalance`,
`calculatedBalance`, `difference` (stored minus calculated), `consistent` and
`checkedEntries`.

The stored balance and the ledger are read in a `REPEATABLE READ`, `READ ONLY`
transaction, so both come from one snapshot. The first version used the default
`READ COMMITTED`, where each statement sees its own snapshot. Under concurrent
bets, 25 of 300 reconciliations of a healthy wallet reported a false divergence
(an operation committed between the two reads).
`TestReconcileUsesOneSnapshotUnderConcurrentWrites` reproduces that, and passes
with the single snapshot. A read-only transaction at this level never fails with
serialization errors, and `READ ONLY` guarantees in the database that
reconciliation changes nothing.

A divergence is reported in three places: the response (`consistent: false` and
the difference); an error log `reconciliation_divergence` with `walletId`,
`difference`, `currency` and `checkedEntries`; and the counter
`reconciliation_divergences_total`, next to `reconciliation_total`. The wallet is
never repaired automatically. There is no correction endpoint; because the
ledger is append-only, a correction would have to be a new ledger entry. Metrics are per process: each replica serves
its own `/metrics`, and a scraper collects all replicas.

### Concurrency and wallet locking

Coordination is per wallet, with pessimistic row locks inside the operation's
transaction; there is no global or in-process lock. An operation:

1. inserts its `wager_transactions` row (`ON CONFLICT DO NOTHING` on the
   idempotency and external-ID unique indexes, which makes duplicates of one
   operation wait for and then replay the first one);
2. for reversals, locks the referenced transaction row (`FOR UPDATE`);
3. locks the wallet row with `SELECT ... FOR NO KEY UPDATE`, applies the domain
   debit or credit, updates balance and version, inserts the ledger entry and
   the outbox events, and commits.

The pending-reference worker takes the same order (reference, then wallet). The
wallet lock is `FOR NO KEY UPDATE`, not `FOR UPDATE`. It is exclusive between
writers, so two operations on one wallet run one after the other and no update
is lost. But it does not conflict with the `FOR KEY SHARE` lock that step 1
takes on the wallet through the foreign key. With `FOR UPDATE`, two concurrent
operations on the same wallet deadlocked: each held `KEY SHARE` from its insert
and waited for the other's to upgrade to `FOR UPDATE`. PostgreSQL aborted one of
them, and the client got a `503`. The E2E 80+80 test found this; the regression
test `TestProcessTransactionConcurrentBetsDoNotDeadlock` fails with SQLSTATE
`40P01` on the old lock and passes on the new one.

Balance non-negativity, ledger uniqueness and immutability do not depend on
these locks: they are database constraints and triggers.
`TestProcessTransactionDifferentWalletsDoNotWaitForEachOther` holds one wallet
locked and shows that an operation on another wallet completes meanwhile.

### Verification of concurrency and failure scenarios

| Scenario | Evidence |
| --- | --- |
| Same bet 50 times in parallel | `TestProcessTransactionFiftyConcurrentDuplicateBets`; E2E `TestE2EFiftyParallelIdenticalBets` through Nginx and three replicas: one debit, 49 replays |
| Two 80.00 bets on 100.00 | `TestProcessTransactionConcurrentBetsLockOneWallet` (plus resends); E2E `TestE2EConcurrentOverdraftBets`: one processed, one `insufficient_funds` (422), balance 20.00, one debit, resends unchanged |
| Different wallets in parallel | `TestProcessTransactionDifferentWalletsDoNotWaitForEachOther`; E2E `TestE2EIndependentWalletsInParallel` |
| Three independent processes | The E2E suite runs against three Compose replicas |
| Consumer stops after commit, before delete | `TestFinancialQueueConsumerRecoveryAfterCommit`; E2E `TestE2EInstanceFailuresDuringSQSLoad` freezes a replica holding an undeleted message and kills it with SIGKILL. Across runs it caught interruptions both after the commit (the Inbox stopped the redelivery) and before it (a survivor applied it) |
| SIGTERM during load | Same E2E test: the stopped replica leaves no received message unfinished; unit tests cover completing the batch and releasing visibility at the deadline |
| Repeated delivery | `TestFinancialQueueConsumerAppliesRepeatedDeliveryOnce`: two real SQS deliveries of one `messageId`, one effect, Inbox duplicate counted |
| Same operation via HTTP and SQS | E2E `TestE2ESameOperationThroughHTTPAndSQS`: concurrent HTTP and SQS plus a later SQS copy with another `messageId`, no conflict (same payload hash on both channels), one debit |
| Conflicting payload via SQS | `TestFinancialQueueConsumerSendsIdempotencyConflictToDLQ` |
| Two publishers, publish then crash before mark | `TestTwoOutboxPublishersPublishConcurrentEventsOnce`, `TestOutboxPublisherRecoversAfterPublishBeforeMarkPublished` (same `eventId` on republication) |
| Reversal before its reference, expiry | `TestReferenceWorkersResolveReversalRegisteredByStoppedInstance`, `TestReferenceWorkerRejectsExpiredReferenceWithObservedBalance` |
| Restart | E2E `TestE2ERestartPreservesIdempotency`: after restarting every replica, replays return the original result and a pending reference is still pending |
| Ledger vs stored balance | Every E2E wallet is checked through `POST /wallets/{id}/reconciliation` |

Operations are always accepted synchronously, without an intermediate `PENDING`
commit (`PENDING` exists only inside the transaction), so there is no committed
`PENDING` to resume. The durable pending state is `PENDING_REFERENCE`, which the
reference worker resumes on any replica.

### Multiple instances

Compose runs three application replicas by default (`deploy.replicas: 3`). Each
instance has its own memory, connection pool, consumers, publisher, and
pending-reference worker, while financial state is shared through PostgreSQL
and SQS. No component relies on in-process state for correctness: idempotency,
pending references and outbox claims are rows, wallet coordination uses row
locks, and each outbox publisher has a random owner token. An Nginx reverse
proxy provides the single external HTTP endpoint. It re-resolves the Docker DNS
name every 2 seconds, so scaled or restarted replicas receive traffic.

When a replica dies, its address stays in Nginx until the next DNS refresh, and
`connect()` to it only failed after about 38 seconds (measured), returning 502.
Nginx therefore uses a 1-second connect timeout and passes the request to
another replica on connection errors and timeouts (`proxy_next_upstream error
timeout`, up to 3 tries). With that, a request that hits a dead replica answered
`200` in about 1 second. For `POST`, Nginx does not resend a request after a
timeout (`non_idempotent` is deliberately not enabled, because a read timeout
would resend a request that may have been processed). So a `POST` can still get
`502`/`504` in that short window. Clients treat `502`, `503` and `504` as
transient and retry the same request. Every `POST` of the API is safe to retry:
wagering through its `Idempotency-Key`, reconciliation is read-only, and opening
a wallet that already exists answers `409`. The E2E client does exactly this.

Verified with a clean Compose project:

- `docker compose up -d --build --wait` became ready in about 40 seconds;
- the full integration suite passed against it;
- 30 requests were balanced 10/10/10 across the replicas;
- 60 outbox events created through HTTP were published by all three
  publishers (20/25/20), with no duplicate claims.

`SIGTERM` (`docker stop`) shuts an instance down in about 0.4 s with exit
code 0. The order is: HTTP server, then messaging workers (cancelling the SQS
long poll), then the PostgreSQL pool. That fits Docker's 10-second grace period.

Compose health checks: PostgreSQL (`pg_isready`), LocalStack (all three queues
resolvable), Keycloak (the `backend` realm discovery document answers `200`, so
the import has finished) and Nginx (`/health/ready` through the proxy). The
application image is distroless and has no shell, so it has no Compose health
check of its own; Nginx's check covers it end to end. Application and Nginx use
`restart: unless-stopped`.

## Observability

HTTP requests and asynchronous worker transitions use a small structured JSON
logger. Logs contain UTC timestamps, level, event name, safe identifiers and
durations when available. `Correlation-ID` is propagated through the request
context and response; a UUID is generated when the client does not provide
one. Authorization headers, tokens, secrets, passwords and complete financial
payloads are not fields in the logger API.

`GET /metrics` exposes Prometheus-compatible counters and summaries. The
registry records operation results, Inbox duplicates, consumer and reference
retries/failures, outbox claims/publications/failures, reconciliation runs and
divergences, plus HTTP request and outbox processing timing. Individual
wallet, transaction, provider, message and correlation IDs are deliberately
not metric labels, so series cardinality remains bounded.

## Database migrations

Database changes are versioned in embedded `up` and `down` SQL files. Startup runs
pending migrations under a PostgreSQL advisory lock, up to the highest version
among the embedded files. A missing version in the sequence stops startup.
Each version, together with its `schema_migrations` row, runs in its own
transaction; PostgreSQL DDL is transactional, so a failing migration leaves no
partial schema. Tests cover concurrent `up` from two runners, revert of the
latest versions, and a full round trip (revert to `000001`, then apply again)
with existing data: the data is kept and the later protections (reversal
uniqueness, outbox immutability) come back. The `cmd/migrate` command is tested
against an isolated schema.

Limitation: a binary does not reject a database that already has versions newer
than the ones it embeds; it applies nothing and starts. Rolling back the
application code therefore requires reverting its newer migrations first. The standalone `cmd/migrate`
command can explicitly apply or revert versions, using the same lock and reversing
versions in order.
