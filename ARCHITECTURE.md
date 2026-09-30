# Architecture

The current runtime is composed with Uber Fx:

```text
HTTP -> application -> ports -> PostgreSQL infrastructure
                                      |
                                      +-> transactional outbox -> Publisher -> SQS FIFO
```

The domain packages do not depend on HTTP, PostgreSQL, AWS SDK, or LocalStack.
Financial processing remains in the existing application and PostgreSQL
repository flow. Wallet locking, ledger writes, wager transaction state, and
outbox events remain in the same PostgreSQL transaction.

The transactional outbox is the bridge between the database transaction and the
SQS FIFO queue. When a wallet or wager transaction is processed, the financial
state and the corresponding outbox record are inserted in the same SQL
transaction. This keeps the money movement durable even when publishing happens
outside the financial transaction.

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
  "type": "WalletBalanceChanged",
  "aggregateId": "...",
  "correlationId": "...",
  "causationId": "...",
  "timestamp": "2025-01-01T00:00:00Z",
  "version": 1,
  "payload": { ... }
}
```

This stage covers only the transactional outbox and the publisher. SQS consumer,
inbox processing, retry workers beyond the outbox publisher, and reconciliation
remain for the next stage. The future inbox/consumer will complete idempotent
processing against the duplicate-delivery window introduced by the outbox.
