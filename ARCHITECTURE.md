# Architecture

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
configured provider claim, so a body field cannot switch providers. Wallet
creation and reads require the `wallet-internal` role and are restricted to
the `backend-internal` service account; provider users do not receive wallet
roles. The public `/health/live` endpoint reports process liveness. The public
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

The application also runs a pending-reference worker for
`PENDING_REFERENCE`. It polls due rows from `wager_transactions`, claims one
row with `FOR UPDATE SKIP LOCKED`, and keeps that lock while the existing
reference retry transaction completes. The persisted
`reference_next_attempt_at` and `reference_attempts` values continue to control
exponential backoff and the existing attempt limit. Because the pending state
is stored in PostgreSQL, a new application instance can recover it after a
restart, and multiple instances can process different pending rows safely.

The Inbox is the durable protection against duplicate delivery and does not
rely on SQS deduplication alone.

The application can run as multiple independent Compose instances. Each
instance has its own memory, connection pool, consumers, publisher, and
pending-reference worker, while financial state is shared through PostgreSQL
and SQS. An Nginx reverse proxy provides the single external HTTP endpoint and
forwards requests to the scaled application service.
