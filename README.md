# Backend Challenge Go

Distributed processing of wagering operations for game providers, in Go with
Uber Fx. An HTTP API and an SQS consumer share one financial use case. Both move
player wallets in PostgreSQL with an append-only ledger, persistent idempotency,
an Inbox and a transactional Outbox. Every request is authenticated against
Keycloak (OAuth 2.0 / OIDC).

`ARCHITECTURE.md` explains the design decisions. This file explains how to run,
call and test the service.

## Prerequisites

- Docker with Docker Compose v2 (`docker compose`). Free ports: 8080 (API),
  8081 (Keycloak), 5432 (PostgreSQL), 4566 (LocalStack).
- Go 1.27.1 (the version in `go.mod` and the `Dockerfile`), to run the tests
  and the tools locally.
- `go test -race` needs cgo and a C compiler. Without gcc (common on Windows),
  run it in a container (see [Tests](#tests)).

## Quick start

```sh
docker compose up -d --build --wait
```

`--wait` returns once PostgreSQL, LocalStack (with its queues), Keycloak (with
the imported realm) and Nginx (which proxies `/health/ready`) are healthy. From
a clean checkout this takes about 40 seconds after the images are built. The
stack contains:

| Service | Address | Notes |
| --- | --- | --- |
| API | `http://localhost:8080` | Nginx in front of **three application replicas** (`deploy.replicas: 3`); each applies pending migrations on startup |
| Keycloak | `http://localhost:8081` | realm `backend`, imported from `keycloak/backend-realm.json` |
| PostgreSQL | `localhost:5432` | database `betting`, user/password `postgres` |
| LocalStack (SQS) | `http://localhost:4566` | `wager-transactions.fifo`, `wager-transactions-dlq.fifo`, `wager-events.fifo`, created by `localstack/init/ready.d/01-init-sqs.sh` |

```sh
docker compose up -d --scale application=5   # another number of replicas
docker compose logs -f application           # JSON logs of every replica
docker compose down                          # stop (keeps the PostgreSQL volume)
docker compose down -v                       # stop and delete all data
```

If you change `keycloak/backend-realm.json`, run `docker compose down` first.
Keycloak imports the realm only when its container is created.

## Configuration

The application reads these environment variables. `docker-compose.yml` sets
them for the containers. The values of `AWS_*`, `OIDC_*` and `SQS_*` can be
overridden in a `.env` file (copy `.env.example`), whose values are written for
the Compose network.

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/betting` | PostgreSQL connection string (pgx) |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `AWS_REGION` | `us-east-1` | SQS region |
| `AWS_ENDPOINT` | empty (real AWS) | SQS endpoint, `http://localstack:4566` in Compose |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | empty | SQS credentials (`test` / `test` for LocalStack) |
| `OIDC_ISSUER_URL` | `http://localhost:8081/realms/backend` | Expected token issuer (the public Keycloak URL) |
| `OIDC_JWKS_URL` | `http://localhost:8081/realms/backend/protocol/openid-connect/certs` | Signing keys; `http://keycloak:8080/...` inside Compose |
| `OIDC_AUDIENCE` | empty | Required audience (`backend-api` in Compose). **When empty, the audience is not checked.** Always set it outside local development |
| `OIDC_PROVIDER_CLAIM` | `provider_id` | Token claim holding the provider identity |
| `SQS_TRANSACTION_QUEUE` | `wager-transactions.fifo` | Command queue |
| `SQS_TRANSACTION_DLQ` | `wager-transactions-dlq.fifo` | Dead-letter queue of the command queue |
| `SQS_EVENT_QUEUE` | `wager-events.fifo` | Output queue of the integration events |
| `SQS_MAX_RECEIVE_COUNT` | `5` | Receives before a command moves to the DLQ (LocalStack redrive and consumer metrics) |

The issuer is the public URL (`localhost:8081`) because that is the address the
token was issued for. The containers fetch the keys from `keycloak:8080`.

## Authentication

Keycloak issues every token; the API never handles passwords. Test identities:

| Identity | Grant | Role | Can call |
| --- | --- | --- | --- |
| `provider-a` / `provider-a`, `provider-b` / `provider-b` (client `backend-api`) | `password` | `wagering-provider`, claim `provider_id` | `POST /wagering/transactions`, `GET /providers/{own id}/...`, `GET /wagering/transactions/{id}` (own only) |
| client `backend-internal`, secret `backend-internal-secret` | `client_credentials` | `wallet-internal` | wallets, ledger, reconciliation, `GET /wagering/transactions/{id}` (any) |

```sh
curl -s -X POST http://localhost:8081/realms/backend/protocol/openid-connect/token \
  -d grant_type=password -d client_id=backend-api -d username=provider-a -d password=provider-a

curl -s -X POST http://localhost:8081/realms/backend/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=backend-internal -d client_secret=backend-internal-secret
```

Use the `access_token` as `Authorization: Bearer <token>`. The provider of a
wagering operation is taken only from the token. A `providerId` in the body is
optional, and if it differs from the token the request is refused with
`403 provider_mismatch`.

## Using the API

The examples use `$INTERNAL` and `$PROVIDER` for the two tokens above. The
responses are real outputs of the stack.

Open a wallet (internal service):

```sh
curl -s -X POST localhost:8080/wallets -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
```

```json
{"balance":{"amount":"1000.00","currency":"BRL"},"id":"4fccdbd9-1439-4ddc-a375-cf5e047001b6","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","version":1}
```

Submit a bet (provider). The `Idempotency-Key` header is required:

```sh
curl -s -X POST localhost:8080/wagering/transactions -H "Authorization: Bearer $PROVIDER" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
       "walletId":"4fccdbd9-1439-4ddc-a375-cf5e047001b6","roundId":"round-987","gameId":"fortune-chimp",
       "kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

```json
{"balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false,"status":"PROCESSED","transactionId":"0797b665-655b-45a7-8d37-a3d3ee6ccb6e"}
```

Sending it again returns the stored result with `"idempotentReplay":true`, and
the balance observed originally. For `REFUND` and `ROLLBACK`, add
`referenceExternalTransactionId`. `LOSS` requires `"amount":"0.00"`.

Reads and reconciliation:

```sh
curl -s localhost:8080/wallets/$WALLET -H "Authorization: Bearer $INTERNAL"
curl -s "localhost:8080/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $INTERNAL"   # then ?cursor=<nextCursor>
curl -s localhost:8080/wagering/transactions/$TRANSACTION -H "Authorization: Bearer $PROVIDER"
curl -s localhost:8080/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER"
curl -s -X POST localhost:8080/wallets/$WALLET/reconciliation -H "Authorization: Bearer $INTERNAL"
```

```json
{"calculatedBalance":{"amount":"975.00","currency":"BRL"},"checkedEntries":2,"consistent":true,"difference":{"amount":"0.00","currency":"BRL"},"storedBalance":{"amount":"975.00","currency":"BRL"},"walletId":"4fccdbd9-1439-4ddc-a375-cf5e047001b6"}
```

The ledger is ordered newest first by `(created_at, id)`. `limit` goes from 1 to
100 (default 50), and `nextCursor` is opaque and empty on the last page.
Transaction reads show the status, `failureCode`, reference and the result
returned to the provider, so pending operations can be followed.

### HTTP contract

Errors use `{"error":"<code>"}` with `Content-Type: application/json`.

| Situation | Status | Body |
| --- | --- | --- |
| Operation processed (also `LOSS`) | `200` | `status: PROCESSED`, `balance`, `transactionId`, `idempotentReplay` |
| Wallet opened | `201` | wallet with `id`, `balance`, `version: 1` |
| Waiting for the reference of a reversal | `202` | `status: PENDING_REFERENCE`, `failureCode: reference_pending`, no balance |
| Malformed or invalid input (nothing is stored) | `400` | `invalid_json`, `idempotency_key_required`, `invalid_player_id`, `invalid_wallet_id`, `invalid_money`, `invalid_request` (unknown kind, `OPENING`, missing reference), `invalid_limit`, `invalid_cursor`, `invalid_transaction_id` |
| Missing, invalid or expired token | `401` | `authentication_required`, `invalid_token` |
| Wrong role, other provider's data, body provider differs from token | `403` | `forbidden`, `provider_access_denied`, `provider_mismatch` |
| Unknown wallet or transaction | `404` | `wallet_not_found`, `transaction_not_found` |
| Same `Idempotency-Key` with another payload, or same external ID with another key | `409` | `{"status":"CONFLICT","failureCode":"idempotency_conflict" \| "external_transaction_conflict","idempotentReplay":false}` |
| Second wallet for the same player and currency | `409` | `wallet_already_exists` |
| Business rejection (stored, terminal) | `422` | `status: REJECTED`, `failureCode`, `transactionId`, `balance` (see the failure codes in `ARCHITECTURE.md`) |
| Temporary unavailability (database unreachable, deadlock, timeout) | `503` + `Retry-After: 1` | `service_unavailable`; nothing was committed, retry with the same `Idempotency-Key` |
| Unexpected error | `500` | `internal_error` |

`GET /health/live` (200 while the process runs) and `GET /health/ready` (200,
or 503 with the failing dependency) are public. A request that reaches a replica
that has just died can get a `502`/`504` from Nginx. Like `503`, it is transient
and safe to retry with the same key.

## SQS commands

Operations can also arrive on `wager-transactions.fifo`. They go through the
same use case and idempotency as HTTP. `data.idempotencyKey` plays the role of
the header, and `messageId` is the Inbox identity of the message:

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "4fccdbd9-1439-4ddc-a375-cf5e047001b6",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

Use the wallet ID as `MessageGroupId`, so one wallet's commands stay ordered and
different wallets run in parallel. Use the `messageId` as
`MessageDeduplicationId`. Example with LocalStack:

```sh
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id <walletId> --message-deduplication-id msg-123 --message-body "$(cat command.json)"
docker compose exec localstack awslocal sqs get-queue-attributes --attribute-names All \
  --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo
```

A message is deleted only after its transaction commits. Business rejections are
terminal and deleted. Invalid messages, idempotency conflicts and failures that
persist through every attempt go to the DLQ after `SQS_MAX_RECEIVE_COUNT`
receives (visibility timeout 30 s, retries 1 to 30 s apart). Results are
published as events on `wager-events.fifo` (contract in `ARCHITECTURE.md`).

Each queue has an access policy, versioned in `localstack/policies/` and applied
by the init script. Providers may only send commands; only the application
consumes commands and publishes events (see `ARCHITECTURE.md`, section 10).
LocalStack Community stores these policies but does not enforce IAM.

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
| `000005_opening_once_per_wallet` | At most one `OPENING` (initial credit) per wallet |
| `000006_one_reversal_per_reference` | At most one processed `REFUND`/`ROLLBACK` per reference, across kinds |

**Apply.** Every replica applies the pending versions on startup, under a
PostgreSQL advisory lock, so concurrent replicas apply each version once. Each
version runs in its own transaction, and `up` is a no-op when nothing is
pending.

**Revert.** `down -steps N` reverts the latest N versions, newest first, each in
its own transaction and under the same lock. Stop the application first:
running replicas expect the latest schema, and a replica that starts applies the
reverted versions again. Reverting `000001_init` drops every financial table and
its data. That cannot be undone, and `schema_migrations` is left empty, not
dropped.

With the Compose stack (no local Go needed; the image ships `/app/migrate`):

```sh
docker compose stop application
docker compose run --rm --no-deps --entrypoint /app/migrate application -direction down -steps 1
docker compose run --rm --no-deps --entrypoint /app/migrate application -direction up
docker compose up -d
```

In Git Bash on Windows, prefix these commands with `MSYS_NO_PATHCONV=1` so
`/app/migrate` is not rewritten as a Windows path. With Go, against
`DATABASE_URL`:

```sh
go run ./cmd/migrate -direction up
go run ./cmd/migrate -direction down -steps 1
```

The command exits with `0` on success, `1` on a database or migration failure,
and `2` on invalid usage.

## Tests

| Command | What it needs | What it runs |
| --- | --- | --- |
| `go vet ./...` | nothing | static checks |
| `go test ./...` | nothing | unit tests. Tests that need PostgreSQL, SQS or Keycloak **skip** when they are not reachable, so a green run here does not prove the integration |
| `go test -tags=integration -count=1 ./...` | the Compose stack | unit and integration tests against the real PostgreSQL, LocalStack and Keycloak (about 245 tests) |
| `go test -race -tags=integration -count=1 ./...` | the stack, cgo | the same with the race detector |
| `go test -tags=e2e -count=1 -v ./test/e2e/` | the stack and the `docker` CLI | end-to-end scenarios through Nginx and the three replicas |

Prepare the dependencies with `docker compose up -d --build --wait`. The tests
use `localhost` and the Compose defaults. Point them elsewhere with
`DATABASE_URL`, `AWS_ENDPOINT` and `OIDC_*`. Run with `-v` and check that the
integration tests report `PASS`, not `SKIP`.

Without a local C compiler, run the race detector in a container on the host
network:

```sh
docker run --rm --network host -v "$PWD":/src -w /src \
  -e DATABASE_URL="postgres://postgres:postgres@localhost:5432/betting?sslmode=disable" \
  golang:1.27.1 go test -race -tags=integration -count=1 ./...
```

Integration tests that change shared state (claims of pending references, the
outbox, migrations) run in their own PostgreSQL schema, so they do not interfere
with the running replicas or with each other.

### Multiple instances and failure simulations

The E2E suite (`test/e2e`, under a minute) runs against the three replicas:

- 50 identical bets in parallel (one debit);
- two 80.00 bets on 100.00 (one processed, one `insufficient_funds`, balance 20.00);
- independent wallets in parallel;
- the same operation sent through HTTP and SQS at once;
- one replica killed with SIGKILL while it holds an unfinished message, and
  another stopped with SIGTERM, during SQS load;
- a restart of every replica.

It uses the `docker` CLI and restores the stack at the end. Every wallet is
checked against the reconciliation endpoint.

Manual simulations on the running stack:

```sh
docker kill --signal KILL <application container>   # crash a replica; SQS redelivers its messages after 30 s
docker stop <application container>                  # graceful SIGTERM: finishes the received batch
docker compose stop postgres                         # API answers 503, readiness 503, liveness 200; then:
docker compose start postgres                        # everything resumes without restarting the API
docker compose stop localstack                       # HTTP keeps working; events wait in the outbox
docker compose start localstack                      # pending events are published
docker compose up -d                                 # bring stopped replicas back
```

Individual scenarios also have integration tests that can be run alone, for
example the poison message that reaches the DLQ:

```sh
go test -tags=integration -count=1 -v -run TestFinancialQueueConsumerRetriesPoisonMessageToDLQ ./internal/application/
```

## Observability

Logs are JSON lines on each replica's output (`docker compose logs
application`). Every HTTP request and SQS command can be followed by its
`correlationId` (the `Correlation-ID` header, or the SQS `messageId`) together
with `transactionId`, `walletId`, `providerId` and `messageId`. Tokens, secrets,
player IDs, amounts and payloads are never logged.

`GET /metrics` exposes Prometheus metrics per replica. They cover results by
status, rejections by failure code, duplicates, retries, DLQ, wallet lock
contention, outbox lag and backlog, processing latency, reconciliation
divergences, readiness failures and authentication failures.

## Project layout

```text
cmd/api                  Fx composition and HTTP/worker lifecycle
cmd/migrate              migration command (also /app/migrate in the image)
internal/domain          Money, Wallet, WagerTransaction, ledger entry (no infrastructure dependencies)
internal/application     wagering and wallet use cases, SQS consumer, outbox publisher, reference worker
internal/ports           interfaces between application and infrastructure
internal/infrastructure  PostgreSQL (pgx, SQL, migrations) and SQS adapters
internal/httpapi         HTTP handlers and health checks
internal/auth            OIDC verification and role-based authorization
internal/messaging       command and event contracts
internal/observability   JSON logger and metrics
test/e2e                 end-to-end tests against the Compose stack
```
