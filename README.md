# Backend Challenge Go

## Local infrastructure

Copy `.env.example` to `.env` when running the stack with custom values. The
default Compose values are local-only credentials for PostgreSQL and LocalStack.

Start PostgreSQL, LocalStack/SQS, and the API:

```sh
docker compose up --build
```

The stack provides:

- PostgreSQL at `localhost:5432`;
- LocalStack at `localhost:4566`;
- the API at `localhost:8080`;
- `wager-transactions.fifo`;
- `wager-transactions-dlq.fifo`.
- Keycloak at `http://localhost:8081`, with realm `backend`.

The protected wagering endpoint requires a Bearer token issued by
Keycloak. Wallet operations are restricted to the internal service client. The development realm imports users `provider-a` and `provider-b`;
their passwords match their usernames. Obtain a token with:

```sh
curl -X POST http://localhost:8081/realms/backend/protocol/openid-connect/token \
  -d grant_type=password -d client_id=backend-api \
  -d username=provider-a -d password=provider-a
```

The API validates the token signature against Keycloak JWKS, issuer, expiry,
and the configured audience. The `provider_id` claim is the only source of
provider identity for wagering. A `providerId` in the HTTP body is optional;
when present it must equal the token's `provider_id`, otherwise the request is
refused with `403 {"error":"provider_mismatch"}` before any processing.
Wallet operations (creation, reads, ledger and reconciliation) require the
`wallet-internal` realm role, granted only to the `backend-internal` service
account. Wagering operations require the `wagering-provider` realm role, granted
to `provider-a` and `provider-b`; the internal service cannot submit them.
Health checks remain public.

LocalStack creates both FIFO queues from
`localstack/init/ready.d/01-init-sqs.sh`. The transaction queue has a redrive
policy to the FIFO DLQ and uses content-based deduplication. The receive limit
is controlled by `SQS_MAX_RECEIVE_COUNT`.

Check the queues:

```sh
docker compose exec localstack awslocal sqs list-queues
docker compose exec localstack awslocal sqs get-queue-attributes \
  --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo \
  --attribute-names All
```

Stop the environment:

```sh
docker compose down
```

## Migrations

Migrations are versioned SQL files embedded in the binaries
(`internal/infrastructure/database/migrations/sql`, one `up` and one `down` per
version):

| Version | Change |
| --- | --- |
| `000001_init` | Wallets, wager transactions, append-only ledger (triggers), Inbox and Outbox |
| `000002_reversal_uniqueness` | At most one processed `REFUND`/`ROLLBACK` per reference and kind |
| `000003_outbox_last_error` | `outbox_events.last_error` |
| `000004_outbox_immutable_snapshot` | Trigger that keeps the outbox event snapshot immutable |

**Apply.** Every application instance applies the pending versions on startup,
under a PostgreSQL advisory lock, so concurrent instances apply each version once.
They can also be applied explicitly. Each version runs in its own transaction,
and `up` is a no-op when everything is already applied.

**Revert.** `down -steps N` reverts the latest N applied versions, newest first,
each in its own transaction and under the same lock. Stop the application before
reverting: running instances expect the latest schema, and a restarted instance
applies the reverted versions again. Reverting `000001_init` drops every
financial table and its data. That cannot be undone, and `schema_migrations` is
left empty, not dropped.

With the Compose stack (no local Go needed; the image ships `/app/migrate`):

```powershell
docker compose up -d postgres
docker compose stop application
docker compose run --rm --no-deps --entrypoint /app/migrate application -direction down -steps 1
docker compose run --rm --no-deps --entrypoint /app/migrate application -direction up
docker compose up -d --scale application=3
```

In Git Bash on Windows, prefix those commands with `MSYS_NO_PATHCONV=1` so
`/app/migrate` is not rewritten as a Windows path.

With a local Go toolchain, using `DATABASE_URL` (default
`postgres://postgres:postgres@localhost:5432/betting?sslmode=disable`):

```powershell
go run ./cmd/migrate -direction up
go run ./cmd/migrate -direction down -steps 1
```

The command exits with `0` on success, `1` on a database or migration failure,
and `2` on invalid usage (unknown direction or `-steps` below 1).

For the multi-instance checks, keep one PostgreSQL/LocalStack/Keycloak stack and
scale only the API workers behind Nginx:

```powershell
docker compose up -d --build --scale application=3
```

The recommended verification commands are:

```powershell
go test ./...
go test -race ./...
go vet ./...
```

Integration tests use the real PostgreSQL, Keycloak and LocalStack services when
run with `-tags=integration`.

## Observability

The API emits one JSON log entry per HTTP request and structured JSON entries
for consumer, outbox, and reference-worker transitions. Entries include UTC
timestamps, level, event message, duration when applicable, and only the
identifiers available to that flow (`correlationId`, `messageId`,
`transactionId`, `walletId`, `providerId`, or `eventId`). The incoming
`Correlation-ID` is preserved and returned; requests without one receive a
generated value.

Authorization headers, JWTs, client secrets, AWS credentials, passwords,
access tokens, and complete financial payloads are never logged.

`GET /metrics` is a public Prometheus-compatible technical endpoint. Counters
cover operation results, Inbox duplicates, retries, consumer failures, outbox
claims/publications/failures, reconciliation executions and divergences.
Request and outbox timing are exported as summaries. Metrics use fixed names
without individual IDs as labels, preventing unbounded cardinality.

## Outbox publisher

This version implements the transactional outbox and the publisher worker. Every
financial mutation still writes the wallet and wager data and inserts the
corresponding outbox event in the same PostgreSQL transaction. The publish step
runs outside the financial transaction in a dedicated worker.

The worker claims a small pending batch with PostgreSQL lock-skipping semantics,
uses the `aggregate_id` as the FIFO `MessageGroupId`, and reuses the stable
`event_id` as `MessageDeduplicationId` so retries remain idempotent at the
message layer. On failures, it schedules a retry with exponential backoff.
The envelope, each event's payload, the aggregate/`MessageGroupId` routing and
the consumption rules are documented in `ARCHITECTURE.md`
("Integration event contract").

The `wager-transactions.fifo` queue is the input command queue. Its messages
have type `WagerTransactionRequested` and are processed by the same financial
use case used by HTTP. The Outbox Publisher publishes the resulting events to
the separate `wager-events.fifo` output queue. The consumer uses the configured
DLQ/redrive policy for poison messages, and the HTTP API exposes the read-only
reconciliation endpoint described below.

The transaction queue consumer is lifecycle-managed. It uses long polling,
processes independent FIFO message groups in parallel, records durable Inbox
state keyed by `consumer_name` and `message_id`, executes the shared financial
use case, and commits Inbox, wallet, wager transaction, ledger, and Outbox
changes in one PostgreSQL transaction. It deletes a message only after that
commit. Processing failures leave the message for SQS redelivery and the
configured redrive policy.

The command queue is FIFO. Producers should set `MessageGroupId` to the wallet
ID (or another stable wallet-level aggregate key) so messages for the same wallet
remain ordered while different wallets can be processed concurrently.
`messageId` is the application-level Inbox identity; the SQS message ID is only the
delivery identifier.

Command messages use the following envelope:

```json
{
  "messageId": "durable-command-id",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-30T12:00:00Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "external-1",
    "idempotencyKey": "provider-a:external-1",
    "playerId": "00000000-0000-0000-0000-000000000001",
    "walletId": "00000000-0000-0000-0000-000000000002",
    "roundId": "round-1",
    "gameId": "game-1",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

`messageId` is the durable Inbox identity and is distinct from the AWS SQS
message ID. `data.idempotencyKey` protects the financial operation. Invalid
commands are not deleted; SQS visibility and the configured redrive policy
control retry and DLQ transfer.

The consumer follows at-least-once delivery. The adapter extracts
`ApproximateReceiveCount` from each SQS delivery and uses it to calculate a
small exponential visibility backoff: the first failure uses the configured
initial delay, then the delay doubles up to the configured maximum. The
consumer calls `ChangeMessageVisibility` for that message and does not sleep
inside the worker. The default application values are 1 second initially and
30 seconds maximum. The queue's `maxReceiveCount` remains the source of truth
for DLQ transfer; the application does not delete poison messages or implement
a competing attempt limit.

The consumer exposes retry and `sqs_messages_dlq_eligible_total` metrics. The
latter is an observation based on the queue's configured receive-count value;
the actual transfer is still performed by SQS redrive.

Business rejections are persisted by the financial use case and are terminal:
after the transaction commits, the message is deleted. Infrastructure failures,
malformed envelopes, and invalid commands are not deleted. They are retried by
SQS with the visibility backoff and eventually moved by the queue redrive
policy. A SIGTERM cancels polling and waits for the consumer goroutine; an
in-flight message is therefore left available for redelivery unless its
durable transaction already committed and its delete completed.

The Compose LocalStack setup creates a FIFO transaction queue with
`SQS_MAX_RECEIVE_COUNT` (default `5`) and a FIFO DLQ. To observe the poison
message flow, start the dependencies with `docker compose up -d`, then run:

```powershell
$env:DATABASE_URL='postgres://postgres:postgres@localhost:5432/betting?sslmode=disable'
go test -tags=integration ./internal/application -run TestFinancialQueueConsumerRetriesPoisonMessageToDLQ -v -count=1
```

## HTTP read and reconciliation endpoints

Wallet endpoints require the `wallet-internal` role. `POST /wagering/transactions`
and the provider lookup require the `wagering-provider` role, and the lookup path
must match the authenticated `provider_id` claim. `GET /wagering/transactions/{id}`
accepts either role: a provider only reads its own transactions (`403` otherwise)
and the internal service reads any transaction.

- `GET /wallets/{walletId}` returns the current wallet balance and version.
- `GET /wallets/{walletId}/ledger?cursor=...&limit=50` returns a stable,
  descending ledger page. The opaque cursor is based on `(created_at, id)`;
  `limit` defaults to 50 and accepts values from 1 through 100.
- `GET /wagering/transactions/{transactionId}` returns the persisted operation
  result, status, failure code, reference and balance result.
- `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}`
  returns a provider operation only when the path provider matches the
  authenticated identity.
- `POST /wallets/{walletId}/reconciliation` compares the stored balance with
  the signed sum of ledger credits and debits. It returns `storedBalance`,
  `calculatedBalance`, `difference`, `consistent` and `checkedEntries` and
  never changes financial state.

### Internal wallet access

The `backend-internal` client uses OAuth 2.0 `client_credentials`. Its service
account receives the `wallet-internal` realm role. Example token request:

```sh
curl -X POST http://localhost:8081/realms/backend/protocol/openid-connect/token \
  -d grant_type=client_credentials \
  -d client_id=backend-internal \
  -d client_secret=backend-internal-secret
```
