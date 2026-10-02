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
cross providers. The public `/health/live` endpoint reports process liveness. The public
`/health/ready` endpoint checks PostgreSQL and SQS readiness, returning `200`
with `{"status":"ready"}` when both are available and `503` with
`{"status":"not_ready","checks":{"postgres":"ok|error","sqs":"ok|error"}}`
when either dependency is unavailable. The application domain and
`WageringService` do not depend on the OIDC library.

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

On SIGTERM, lifecycle cancellation stops polling and waits for the consumer
goroutine. A message in flight without a completed delete becomes visible
again and is redelivered; a message whose database transaction committed is
protected by the Inbox and will not apply the financial effect twice.

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
not use SQL offsets. Reconciliation runs in a read transaction, sums credits
and debits including the opening entry, reports the difference from the stored
wallet balance, and never mutates the wallet or ledger.

The application can run as multiple independent Compose instances. Each
instance has its own memory, connection pool, consumers, publisher, and
pending-reference worker, while financial state is shared through PostgreSQL
and SQS. An Nginx reverse proxy provides the single external HTTP endpoint and
forwards requests to the scaled application service.

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
among the embedded files. A missing version in the sequence stops startup. The standalone `cmd/migrate`
command can explicitly apply or revert versions, using the same lock and reversing
versions in order.
