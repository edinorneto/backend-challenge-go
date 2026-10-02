# Architecture

This document records the design decisions, how each guarantee of the challenge
is enforced, and the interpretations and limitations. `README.md` covers running,
calling and testing the service.

Contents:

1. [Overview and composition](#1-overview-and-composition)
2. [Domain model](#2-domain-model)
3. [Persistence and SQL transactions](#3-persistence-and-sql-transactions)
4. [Operations, reversals and failure codes](#4-operations-reversals-and-failure-codes)
5. [Idempotency](#5-idempotency)
6. [Concurrency and wallet locking](#6-concurrency-and-wallet-locking)
7. [Pending references](#7-pending-references)
8. [SQS consumer and Inbox](#8-sqs-consumer-and-inbox)
9. [Transactional outbox and integration events](#9-transactional-outbox-and-integration-events)
10. [Authentication and authorization](#10-authentication-and-authorization)
11. [Health, failures and multiple instances](#11-health-failures-and-multiple-instances)
12. [Reconciliation](#12-reconciliation)
13. [Observability](#13-observability)
14. [Database migrations](#14-database-migrations)
15. [Verification of concurrency and failure scenarios](#15-verification-of-concurrency-and-failure-scenarios)
16. [Interpretations, limitations and work not done](#16-interpretations-limitations-and-work-not-done)

## 1. Overview and composition

```text
HTTP (Nginx -> replicas) --\                                    /-> ledger, wallets, transactions
                            >-> WageringService -> WalletRepo --+-> Inbox (SQS path)
SQS wager-transactions.fifo /     (one use case)                \-> outbox_events
                                                                        |
                                    OutboxPublisher (worker) <----------/
                                            |
                                            v
                                    SQS wager-events.fifo
```

HTTP and SQS call the same `WageringService.ProcessTransaction`, so validation,
the idempotency hash, the financial rules and the metrics are identical on both
channels. A financial change (wallet balance and version, wager transaction
state, ledger entry, Inbox completion and outbox events) is committed in one
PostgreSQL transaction. Events are published afterwards by a separate worker.

Packages: `internal/domain` (Money, Wallet, WagerTransaction, ledger entry) has
no dependency on Fx, HTTP, SQS or the database library. `internal/application`
holds the use cases and workers and talks to the infrastructure through
`internal/ports`. `internal/infrastructure` holds PostgreSQL (pgx) and SQS (AWS
SDK v2). `internal/httpapi` and `internal/auth` hold the HTTP and OIDC edge.

**Uber Fx.** `cmd/api` declares one `fx.Module("backend-api")`. It provides
configuration, logger and metrics, the pgx pool, the migration runner, the SQS
client and adapters, the repositories, the use cases, the workers and the HTTP
server, all through constructors. `fx.Invoke` attaches the lifecycle:

| Start order (`OnStart`) | Stop order (`OnStop`, reversed) |
| --- | --- |
| 1. `config.Load` validates the configuration (missing database URL, issuer, queue names, invalid receive count) | 4. HTTP server `Shutdown`: stops accepting connections and waits for in-flight requests |
| 2. pool: `Ping` PostgreSQL (fails the start if unreachable) | 3. consumer (stops polling, completes the received batch), reference worker, outbox publisher, each waiting for its goroutine |
| 3. migrations under an advisory lock; resolve the three queue URLs; start publisher, reference worker and consumer | 2. — |
| 4. HTTP listener | 1. pool `Close`, after every component that uses it has stopped |

Workers start with `context.Background()`, so the start deadline does not
cancel them. Their `Stop` cancels them and waits until their goroutines finish
or the stop deadline expires (Fx default 15 s; Compose gives the container
20 s before SIGKILL). `TestApplicationFxComposition` validates the graph,
`TestHTTPServerExposesSharedMetrics` builds it, and the worker tests check
start, stop, cancellation and that a worker outlives the start context.
Constructors with variadic options (logger, metrics) are wrapped in the module,
because Fx does not inject variadic parameters.

## 2. Domain model

Entities keep their state in unexported fields and change only through methods
that validate the transition. They are created by constructors that validate
every field (`wallet.New`, `wagertransaction.NewExternal`, `NewOpening`,
`ledger.NewDebit`/`NewCredit`) and rebuilt from the database by `Rehydrate`.
Rehydration checks consistency but never applies a movement, a transition or an
event. A zero-value `Money` (no currency) is not a valid value: every constructor and
operation refuses it (`TestDomainRejectsUninitializedMoney`). Business rejections are values, not panics: errors are sentinels (e.g.
`wallet.ErrInsufficientFunds`, `money.ErrCurrencyMismatch`,
`wagertransaction.ErrTerminalTransaction`) checked with `errors.Is`. Every I/O
method takes a `context.Context`.

**Money.** An immutable value with an `int64` number of cents and an ISO-4217-style
currency code (three upper-case letters). Range: ±92,233,720,368,547,758.07.
Overflow is detected and returned as `ErrOverflow` in parsing, `Add`,
`Subtract` and `Negate`. Arithmetic and comparison require the same currency
(`ErrCurrencyMismatch`). No `float32`/`float64` is used anywhere: parsing is done
on the string, and JSON carries amounts as decimal strings (`"25.00"`).
External input (`ParseExternal`) is trimmed and accepts at most two decimals. It
rejects empty values, signs, exponents (`1e3`), `NaN`/`Infinity` and anything
that is not `digits[.d|.dd]`. Equivalent forms are normalized: `25`, `25.5` and
`025.50` all become `25.50` before validation, storage and hashing. Negative
values exist only in internal differences (e.g. reconciliation).

**Wallet.** The aggregate root: ID, player, currency, balance, version, creation
and update times. `New` starts at version `1`. `Debit` and `Credit` check the
currency and, for debits, that the balance stays non-negative. They increment the
version only when the balance changes, so a `LOSS` keeps the version. One wallet
exists per `(playerId, currency)`.

**WagerTransaction.** Kinds `OPENING`, `BET`, `WIN`, `LOSS`, `REFUND`,
`ROLLBACK`. External operations carry the internal and external IDs, provider,
idempotency key, payload hash, wallet, player, round, game, kind, money,
optional reference, status, timestamps, and, when applicable, the resolved
reference, the failure code and the result returned to the provider (balance and
wallet version). `OPENING` is internal: no provider, external ID, key, hash,
round, game or reference (enforced by `chk_wager_opening_internal`).

State machine:

```text
PENDING --> PROCESSED | REJECTED | FAILED            (terminal)
   |
   +----> PENDING_REFERENCE --> PENDING_REFERENCE (retry)
                     |------> PROCESSED | REJECTED | FAILED
```

Terminal states accept no transition (`ErrTerminalTransaction`). A replay reads
the stored result and never re-applies the operation. `PENDING` exists only
inside the transaction that creates the operation: operations are completed
synchronously, so no `PENDING` row is ever committed and there is nothing to
resume. The durable waiting state is `PENDING_REFERENCE`.

**Transient and permanent failures.** A business rule that refuses an
operation gives `REJECTED` with a stored `failureCode`, which is terminal. A
transient infrastructure failure (database unreachable, deadlock, serialization
failure, timeout) rolls the transaction back, so nothing is stored. HTTP answers
`503` and the client retries with the same key; SQS redelivers with backoff. A
permanent infrastructure or input failure on SQS (malformed message, idempotency
conflict, or a failure that lasts through all attempts) ends in the DLQ, also
with nothing stored. `FAILED` is therefore never persisted (see section 16).

**Ledger entry.** Immutable: ID, wallet, transaction, direction (`DEBIT` or
`CREDIT`), amount, balance before and after, creation time. The constructors
check `balanceAfter = balanceBefore ± amount`.

## 3. Persistence and SQL transactions

**Library.** `pgx/v5` with explicit SQL, no ORM. Transactions, row locks and
constraints are visible in `internal/infrastructure/database`. Money is stored
as `BIGINT` cents plus a `CHAR(3)` currency column, so value and currency
are preserved exactly.

**Transaction boundaries.** `WalletRepo.ProcessTransaction` runs the whole
operation in one transaction: the transaction row, reference lock, wallet lock
and update, ledger entry, and outbox events. When it is called on its own (HTTP)
it opens and commits the transaction itself. On the SQS path the consumer opens
the transaction with `TransactionManager.Begin`, which puts the `pgx.Tx` in the
context. The Inbox repository and `ProcessTransaction` both use the transaction
from the context, and the consumer commits once. So the Inbox record, its
completion and the financial change commit or roll back together. Wallet
opening (wallet, `OPENING`, credit entry, two events) is also one transaction.

**Guarantees in the schema** (not dependent on application locks or SQS
deduplication):

| Guarantee | Enforcement |
| --- | --- |
| Non-negative balance | `chk_wallet_balance_non_negative`; ledger `balance_before`/`balance_after` checks |
| One wallet per player and currency | `uq_wallet_player_currency` |
| Unique operation per provider | `uq_wager_provider_idempotency_key`, `uq_wager_provider_external_transaction` |
| One initial credit per wallet | `uq_wager_opening_per_wallet` (migration `000005`) |
| One processed reversal per reference and kind | `uq_wager_processed_reversal_reference_kind` |
| One processed reversal per reference, any kind | `uq_wager_processed_reversal_reference` (migration `000006`) |
| One ledger entry per wallet and transaction | `uq_ledger_wallet_transaction` |
| Append-only ledger | triggers reject every `UPDATE` and `DELETE` (row level) and every `TRUNCATE` (statement level, migration `000007`) on `wallet_ledger_entries` |
| Immutable outbox snapshot | trigger rejects updates of the event columns (`000004`) |
| Internal vs external operations | `chk_wager_source`, `chk_wager_opening_internal`, `chk_wager_external_non_opening`, `chk_wager_reversal_reference` |
| Kinds, statuses, amounts, currency format | `CHECK` constraints |
| Inbox deduplication | `uq_inbox_consumer_message (consumer_name, message_id)` |

## 4. Operations, reversals and failure codes

| Kind | Movement | Amount | Rule |
| --- | --- | --- | --- |
| `BET` | debit | > 0 | balance must cover it |
| `WIN` | credit | > 0 | — |
| `LOSS` | none | exactly `0.00` | no ledger entry, version unchanged; emits `WagerTransactionProcessed` only |
| `REFUND` | credit | > 0 | reference must be a processed `BET`; returns its full amount |
| `ROLLBACK` | opposite of the reference | > 0 | reference must be a processed `BET` (credit), `WIN` (debit) or `REFUND` (debit) |
| `OPENING` | credit | ≥ 0 | internal only; refused on HTTP and SQS; a zero opening creates no `OPENING`, entry or events |

Every operation must use the wallet's currency and belong to that wallet's
player. A reversal resolves `referenceExternalTransactionId` by
`(providerId, externalTransactionId)` and must match the reference's provider,
player, wallet, currency, round and exact amount. Partial reversals are refused.

**REFUND and ROLLBACK on the same bet.** A `BET` can be reversed **once**,
either by one `REFUND` or by one `ROLLBACK`: after either is processed, any
other reversal of that bet is `reversal_already_processed`. The returned debit
can therefore never be credited twice. A `WIN` and a `REFUND` can each be
rolled back once. Rolling back a `REFUND` debits the refunded amount again, and
the bet stays reversed, so it cannot be refunded a second time. The uniqueness
per reference, across kinds, is also a database index (migration `000006`). The
repository checks the same rule first, under the lock of the referenced row,
which serializes reversals of one reference and turns a second one into a
stored `reversal_already_processed` rejection. A reversal that would need to debit more than the balance is
rejected with `reversal_insufficient_funds`, which is distinct from a bet's
`insufficient_funds`.

**Failure codes.** Every rejection is stored with a stable `failureCode`,
returned by HTTP (`422`), the transaction reads and the
`WagerTransactionRejected` event. A rejection consumes the operation's
`externalTransactionId` and key. Replays return it. To try again with corrected
input, the provider sends a new operation with a new external ID and key.

| Code | Meaning | Nature |
| --- | --- | --- |
| `insufficient_funds` | the bet exceeds the balance | definitive result |
| `reversal_insufficient_funds` | the rollback would need a debit larger than the balance | definitive result |
| `reversal_already_processed` | the reference was already reversed | definitive result |
| `reference_not_found` | the reference did not arrive within the retry window | definitive result |
| `reference_incompatible` | the reference differs (provider, player, wallet, round, currency, amount), has a kind that cannot be reversed this way, or ended `REJECTED`/`FAILED` | correctable input |
| `currency_mismatch` | the operation's currency is not the wallet's | correctable input |
| `wallet_player_mismatch` | the wallet belongs to another player | correctable input |

`reference_pending` is not a rejection; it accompanies the `PENDING_REFERENCE`
status. Malformed requests (`400`) and conflicts (`409`) are never stored, so the
same key can be used again with a valid payload. An amount that breaks the
kind's rule (`BET`, `WIN`, `REFUND` or `ROLLBACK` not positive, `LOSS` not
zero) is malformed input: `WageringService` refuses it before anything is
stored, as `400 invalid_request` on HTTP and as an `invalid_message` failure on
SQS (the message reaches the DLQ). It is not a stored `422` rejection.

## 5. Idempotency

The `Idempotency-Key` header (HTTP) or `data.idempotencyKey` (SQS) is required
and used exactly as received; the server never replaces it with a computed one.
Keys and external IDs are unique per provider in the schema. The first request
inserts the transaction row (`INSERT ... ON CONFLICT DO NOTHING`), and every
later or concurrent request with the same key or external ID finds that row:

| Situation | Result |
| --- | --- |
| Same key, same payload hash | the stored result, `idempotentReplay: true`, with the balance and version observed when it was processed (not the current balance) |
| Same key, different payload | `409 idempotency_conflict`, no effect |
| Same `(providerId, externalTransactionId)`, different key | `409 external_transaction_conflict`, no effect |

Because the key lives in PostgreSQL, idempotency survives restarts of every
process and is shared by all replicas and both channels. A concurrent duplicate
waits on the unique index for the first transaction and then replays it.

**Payload hash.** SHA-256 (hex) of the canonical JSON of the business fields:
`externalTransactionId`, `gameId`, `kind`, `money` (`amount`, `currency`),
`playerId`, `providerId`, `referenceExternalTransactionId` (omitted when empty),
`roundId`, `walletId`. Canonical means keys sorted at every level, no
insignificant whitespace and no HTML escaping. For example:

```json
{"externalTransactionId":"tx-1","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"playerId":"…","providerId":"provider-a","roundId":"round-987","walletId":"…"}
```

Values are normalized first: `kind` trimmed and upper-cased, `money.amount` in
the two-decimal form, UUIDs in lower-case canonical form. The idempotency key and
transport metadata (headers, `messageId`, `occurredAt`, `type`) are excluded.
The provider comes from the token on HTTP and from `data.providerId` on SQS.
HTTP and SQS call the same function, which
`TestE2ESameOperationThroughHTTPAndSQS` exercises with the same operation on
both channels, without conflict. `TestWageringPayloadIsCanonicalJSONWithSortedKeys`
fixes the exact canonical string.

## 6. Concurrency and wallet locking

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

## 7. Pending references

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

Each replica runs a reference worker. It polls every second, claims one due row
with `FOR UPDATE SKIP LOCKED` (in `reference_next_attempt_at` order) and keeps
the lock while the retry transaction completes. It takes the same locks as the
synchronous path: first the reference row, then the wallet. The state and the
backoff are rows, so another replica resumes them after a restart, and several
replicas never process the same row. A retry that fails because of
infrastructure is rolled back without consuming an attempt and is picked up
again on the next poll. Metrics: `reference_worker_processed_total` and
`reference_worker_failures_total`.

## 8. SQS consumer and Inbox

The command queue `wager-transactions.fifo` receives `WagerTransactionRequested`
messages (envelope in `README.md`). It has a redrive policy to
`wager-transactions-dlq.fifo` with `maxReceiveCount` = `SQS_MAX_RECEIVE_COUNT`
(5). Each replica long-polls it (20 s wait, 30 s visibility, batches of up to
10). It processes different `MessageGroupId`s in parallel and the messages of one
group in order.

For each message the consumer validates the envelope and the command, then in
one transaction:

1. inserts the Inbox row `(consumer_name, message_id, payload_hash)`. If the
   `messageId` was already completed, it is a duplicate (counted in
   `inbox_duplicates_total`) and has no effect. The same `messageId` with a
   different body is an error;
2. runs `WageringService.ProcessTransaction` with `data.idempotencyKey`;
3. marks the Inbox row completed and commits.

Only then does it delete the message. Delivery is at-least-once: a message whose
delete did not happen is redelivered, and the Inbox (same `messageId`) or the
idempotency key (another `messageId` for the same operation) stops a second
effect. Neither depends on SQS deduplication.

| Outcome | Message |
| --- | --- |
| Processed, rejected (business rule) or pending reference | committed, then deleted |
| Duplicate `messageId` | deleted, no effect |
| Malformed JSON, invalid envelope or command, idempotency conflict | not deleted; reaches the DLQ after the receive limit, no effect |
| Infrastructure failure | not deleted; next delivery delayed with `ChangeMessageVisibility` (1, 2, 4… up to 30 s, from `ApproximateReceiveCount`), DLQ after the limit |

A message in the DLQ was never applied and can be moved back to the command
queue once the cause is fixed; the Inbox and the idempotency key make that
replay safe. The application never deletes a poison message itself, and
`sqs_messages_dlq_eligible_total` only observes that a delivery failed at the
last allowed attempt.

**Shutdown.** On SIGTERM the consumer uses two contexts. The polling context is
cancelled at once, so no new message is received. The batch already received is
completed with a separate work context, so its transactions commit and its
messages are deleted before the process exits. Only if the stop deadline expires
is that work context cancelled: the open transactions roll back and every
message that did not complete has its visibility set to 0, so another replica
receives it immediately. A replica that dies without running this (SIGKILL,
crash) leaves its received messages invisible until the visibility timeout. They
are then redelivered: if the transaction had committed, the Inbox stops the
duplicate; otherwise the survivor applies it.

When a replica stops in the middle of a long poll, LocalStack can still hand a
newly arriving message to that closed request. The message is then delivered
only after the visibility timeout, so a command sent right after replicas are
replaced can take about 30 s. It is still processed exactly once.

## 9. Transactional outbox and integration events

Every financial change inserts its events into `outbox_events` in the same
transaction. Nothing is published before the commit, and nothing committed is
lost if the process dies before publishing. Each replica runs an
`OutboxPublisher`:

- every second it claims up to 10 due rows with `FOR UPDATE SKIP LOCKED` and a
  30 s lease (`locked_at`, `locked_by` = a random owner per process), so
  several publishers never claim the same row;
- it sends each event to `wager-events.fifo` with `MessageGroupId` =
  `aggregateId` and `MessageDeduplicationId` = `eventId`, then marks the row
  `PUBLISHED`. Marking and rescheduling require the lease owner, so a publisher
  whose lease expired cannot overwrite a row another one took;
- a failed send is rescheduled with exponential backoff and the error is kept in
  `last_error`; a lease abandoned by a dead publisher expires and is claimed
  again.

If a publisher dies between the send and the mark, another publisher sends the
event again later with the **same `eventId`**. Consumers must therefore
deduplicate by `eventId`.

**Event contract.** Each event has a concrete Go payload type
(`internal/messaging/events.go`). The constructors (`NewWagerTransactionProcessed`,
`NewWagerTransactionRejected`, `NewWalletBalanceChanged`,
`NewWagerTransactionPendingReference`) are the only place that sets
`eventType`, `version` and the aggregate. The payload is a sealed interface.
Before sending, the publisher validates the envelope: known type, version
matching the contract, IDs present, UTC `occurredAt`, non-null JSON `data`.

| Envelope field | Meaning |
| --- | --- |
| `eventId` | UUID generated when the outbox row is written; reused on every republication and sent as `MessageDeduplicationId` |
| `eventType` / `version` | set by the constructor; all events are at version `1` |
| `aggregateId` | `WalletBalanceChanged`: the wallet ID. The three `WagerTransaction*` events: the internal transaction ID, including `OPENING`. Also the FIFO `MessageGroupId` |
| `correlationId` | the internal transaction ID, shared by all events of one operation whatever the channel. Request and message IDs are in the logs next to `transactionId` |
| `causationId` | optional, currently omitted: no event is caused by another integration event |
| `occurredAt` | UTC RFC 3339 time of the commit that produced the event |

Money values are `{"amount":"25.00","currency":"BRL"}` with decimal strings.

| Event | Trigger | Data |
| --- | --- | --- |
| `WagerTransactionProcessed` | successful operation, including `LOSS` and the internal `OPENING` | `transactionId`, `walletId`, `playerId`, `kind`, `status`, `money`, `result.balance`, `result.version`; for external operations also `providerId`, `externalTransactionId`, `roundId`, `gameId` and, for reversals, `referenceExternalTransactionId` (omitted for `OPENING`) |
| `WagerTransactionRejected` | definitive business rejection, including reference expiry | the same identification fields plus `failureCode`; for expiry also `referenceAttempts`; `result` holds the balance and version observed at rejection |
| `WalletBalanceChanged` | effective balance change (never for `LOSS` or rejections) | `walletId`, `transactionId`, `direction`, `money`, `balanceBefore`, `balanceAfter`, `walletVersion`, `previousVersion` |
| `WagerTransactionPendingReference` | wait for a reference registered (once; retries only advance the backoff) | `transactionId`, `walletId`, `providerId`, `kind`, `status`, `failureCode`, `referenceExternalTransactionId`, `referenceAttempts`, `nextAttemptAt` |

Routing and consumption: every event goes to `wager-events.fifo`, separate from
the command queue, and is never read as a command. Consumers route by
`eventType`, deduplicate by `eventId` (SQS deduplication only covers 5 minutes)
and rely on order only within one `aggregateId`. For example, one wallet's
`WalletBalanceChanged` events arrive in `walletVersion` order, but a
transaction's `WagerTransactionProcessed` and its `WalletBalanceChanged` are not
ordered with respect to each other. The outbox row is an immutable snapshot:
migration `000004` rejects updates of every event column. Only the delivery
state (`status`, `attempts`, `next_attempt_at`, lease, `published_at`,
`last_error`) can change.

## 10. Authentication and authorization

**IdP.** Keycloak 25, in Compose, with the realm, clients, roles and test users
provisioned from `keycloak/backend-realm.json` at startup. It is the IdP the
challenge recommends, it is a standard OIDC provider, and the realm import makes
the environment reproducible. Providers use the `password` grant of the
`backend-api` client (test users only). The internal service uses
`client_credentials` on the confidential `backend-internal` client. The API
never stores passwords or issues tokens. The test-only client
`backend-api-short-lived` issues the same provider tokens with a 2 s lifespan,
so `TestKeycloakExpiredTokenIsRejected` checks expiry against a real Keycloak
token; like the test users, it does not belong in a production realm.

**Validation** (`internal/auth`, `go-oidc`). Every protected route requires a
Bearer JWT. The API checks the RS256 signature against the realm JWKS (fetched
and cached by key ID), the issuer, the expiry and the audience (`backend-api`).
It requires a subject. It reads the realm roles and, for providers, the
`provider_id` claim (configurable). Missing, malformed, expired or wrongly
signed tokens get `401`. The JWKS is fetched lazily, so the API does not depend
on Keycloak to start.

**Permission model.** Realm roles:

| Role | Holder | Allows |
| --- | --- | --- |
| `wagering-provider` | provider users (`provider-a`, `provider-b`), each with its `provider_id` | submit operations; read its own operations (`GET /providers/{id}/...` only for its own ID; `GET /wagering/transactions/{id}` only its own, `403` otherwise) |
| `wallet-internal` | only the `backend-internal` service account | open wallets, read wallets and ledgers, reconcile; read any transaction. It cannot submit wagering operations |

The provider of an operation comes only from the token: a body `providerId`
that differs is refused with `403 provider_mismatch` before any processing.
Below HTTP, keys, external IDs and reference resolution are all scoped by
`provider_id` in the unique indexes and queries. A provider can therefore never
replay, reverse or read another provider's operation, and refused requests have
no financial effect (`TestKeycloakProviderIsolationHasNoCrossProviderEffects`).
Health checks are public. `/metrics` is public in Compose (see section 13).

**Messaging access.** Access to the queues is controlled by credentials and by
queue policies kept in the repository (`localstack/policies/*.json`). The
LocalStack init script applies them to the queues it creates:

| Queue | Allowed |
| --- | --- |
| `wager-transactions.fifo` | `role/wagering-provider-producer`: only `SendMessage`. `role/backend-application`: receive, delete, change visibility |
| `wager-transactions-dlq.fifo` | SQS itself, only for redrive from the command queue (`aws:SourceArn`). `role/backend-application`: read attributes (readiness). `role/backend-operator`: inspect, delete and move messages back (`StartMessageMoveTask`) |
| `wager-events.fifo` | `role/backend-application`: only `SendMessage`. `role/wager-events-consumer`: receive and delete |

Providers can therefore only produce commands, never read them or the events.
Only the application publishes events. The application uses its own
credentials (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`).
`TestLocalStackTransactionQueues` checks that every queue has its policy and that
providers are never allowed to consume. The consumer trusts the authenticated
producer for `data.providerId` and still applies every domain validation (the
wallet's player and currency, reference matching, idempotency). LocalStack
Community stores the policies but does not enforce IAM, so the denial itself is
not exercised locally (section 16).

**Provider identity on SQS.** HTTP derives the provider from the token. On SQS
the message carries `data.providerId`, and the consumer does not bind it to the
sender: the command queue is treated as an **internal, trusted channel**. Who
may publish is decided by the broker, as section 2 of the challenge asks:
credentials plus the queue policy, which lets only the producer role send and
never read. Any authenticated producer can therefore send a command for any
provider. That is acceptable only while the producer role belongs to a trusted
integration layer, not to the providers themselves.

Binding by the `SenderId` system attribute was evaluated and not adopted.
LocalStack fills `SenderId` with the account derived from the access key, which
it does not verify: two producers with different credentials in the same account
get the same `SenderId`, and any caller can pick any key.
`TestLocalStackSenderIdDoesNotIdentifyTheProducer` records this. A check built
on it would pass the tests here without proving anything.

Evolution for production, in order of preference:

1. **One queue per provider.** Each queue's policy allows `SendMessage` only to
   that provider's IAM role, and the consumer takes the provider from the queue
   it read, not from the body. A divergent `data.providerId` goes to the DLQ
   without any effect.
2. **Shared queue with `SenderId`.** On AWS, `SenderId` is the caller's IAM
   principal (the role ID for an assumed role, `AROA...:session`). A
   configuration table maps each role ID to its `providerId`; the consumer
   requests the attribute (`MessageSystemAttributeNames: SenderId`) and sends a
   mismatch or an unknown sender to the DLQ, counted as `invalid_message`.

Either way the domain validations stay as they are (wallet player and currency,
reference matching, idempotency scoped by provider).

## 11. Health, failures and multiple instances

**Health checks.** Both are public and accept only `GET`.

- `/health/live` answers `200` while the process runs. It ignores dependencies,
  so an outage does not make the orchestrator restart replicas that will
  recover by themselves.
- `/health/ready` checks PostgreSQL (`Ping`) and SQS (resolving the three queue
  URLs, so a missing queue also fails). It returns `200` `{"status":"ready"}` or
  `503` `{"status":"not_ready","checks":{"postgres":"ok|error","sqs":"ok|error"}}`.

The two checks run concurrently with a 3 s deadline each, so a hanging
dependency cannot make the other look unavailable. Failures are logged as
`readiness_check_failed` (`dependency`, `reason` timeout or unavailable) and
counted.

**Temporary unavailability.** Replicas keep running when a dependency is down.
A request that fails because of a transient database condition
(`database.IsTransient`: connection failure, dropped connection, timeout,
deadlock, serialization failure, other retryable SQLSTATEs) answers `503`
`service_unavailable` with `Retry-After: 1`. Nothing was committed, so the
client retries with the same key. Other unexpected errors answer `500`. HTTP
writes do not need SQS: events wait in the outbox and are published when it
returns. The SQS consumer, the publisher and the reference worker retry on their
next cycle. This was verified by stopping and restarting PostgreSQL and
LocalStack under three replicas: readiness reported only the affected
dependency, liveness stayed `200`, and everything resumed without restarting the
application.

**Multiple instances.** Compose runs three replicas by default. Each has its own
memory, connection pool, consumer, publisher and reference worker. They share
only PostgreSQL and SQS, and no correctness property depends on process memory.
Nginx is the single HTTP entry point. It re-resolves the replicas' DNS name
every 2 seconds. It fails a connection to a dead replica after 1 s and tries
another one (`proxy_next_upstream error timeout`, up to 3 tries). Without that,
a request to a killed replica hung for about 38 s and returned `502`. Nginx does
not resend a `POST` after a timeout (`non_idempotent` is deliberately off,
because a read timeout would resend a request that may have been processed), so
in that short window a `POST` can get `502`/`504`. Clients retry `502`, `503`
and `504` with the same request. Every `POST` is safe to repeat: wagering
through its key, reconciliation is read-only, and opening an existing wallet
answers `409`.

Verified on a fresh Compose project:

- ready in about 40 s;
- the full integration suite passed against it;
- requests were balanced 10/10/10 across the replicas;
- 60 outbox events were published by all three publishers (20/25/20) with no
  duplicates;
- `docker stop` shut a replica down in about 0.4 s with exit code 0.

Compose health checks cover PostgreSQL, LocalStack (three queues), Keycloak (the
realm discovery document, so the import has finished) and Nginx
(`/health/ready` through the proxy). The application image is distroless, has no
shell and no health check of its own; Nginx's check covers it.

## 12. Reconciliation

`POST /wallets/{walletId}/reconciliation` (role `wallet-internal`) rebuilds the
balance from the ledger (credits minus debits, including the `OPENING` credit).
It compares that with the stored balance and returns `storedBalance`,
`calculatedBalance`, `difference` (stored minus calculated), `consistent` and
`checkedEntries`.

Both values are read in a `REPEATABLE READ`, `READ ONLY` transaction, so they
come from one snapshot. The first version used `READ COMMITTED`, where each
statement sees its own snapshot. Under concurrent bets, 25 of 300
reconciliations of a healthy wallet reported a false divergence;
`TestReconcileUsesOneSnapshotUnderConcurrentWrites` reproduces that and passes
now. A read-only transaction at this level cannot fail with serialization
errors, and `READ ONLY` guarantees in the database that nothing is changed.

A divergence is reported in the response (`consistent: false`), in an error log
`reconciliation_divergence` (`walletId`, difference, currency, entries) and in
`reconciliation_divergences_total`. The wallet is never repaired automatically.
Because the ledger is append-only, a correction would have to be a new ledger
entry; there is no correction endpoint.

## 13. Observability

### Logs

Every log line is one JSON object written by `observability.Logger`, with a UTC
timestamp, level and event name. Fields pass through an allowlist, so tokens,
`Authorization` headers, secrets, passwords, player IDs, amounts and complete
payloads cannot be logged even by mistake. Errors are logged as `errorClass`,
never as their message (which can contain hosts, user names or values):
`timeout`, `canceled`, `postgres_<SQLSTATE>` (e.g. `postgres_40P01`),
`postgres_connect`, `aws_<error code>`, `network`, or the type of the innermost
error.

Correlation: HTTP uses the incoming `Correlation-ID` header (a UUID is generated
when absent and returned in the response). SQS uses the command's `messageId`.
Every line of that request or command carries it as `correlationId`.

| Event | Identifiers |
| --- | --- |
| `http_request` | `correlationId`, method, route, status, duration |
| `wager_transaction_completed` / `wager_transaction_failed` (HTTP and SQS) | `correlationId`, `transactionId`, `walletId`, `providerId`, operation, status, `failureCode`, replay, duration |
| `consumer_command_validated`, `consumer_financial_processing_completed`, `consumer_message_deleted`, `consumer_message_retry_scheduled`, `consumer_message_released` | `correlationId`, `messageId`, `walletId`, `providerId`, `transactionId` when known |
| `outbox_event_claimed`, `outbox_event_published`, `outbox_publish_failed` | `eventId`, `aggregateId`, owner, attempt |
| `reconciliation_divergence` | `walletId`, difference, currency, `checkedEntries` |
| `readiness_check_failed`, `request_failed`, `authentication_failed` | dependency or route, reason |

Events use the transaction ID as their `correlationId`. Each request or command
log line carries both its own `correlationId` and the `transactionId`, so
requests, logs and events can be joined.

### Metrics

`GET /metrics` serves each process's registry in the Prometheus text format.
Every replica has its own counters, so a scraper collects all replicas. The
endpoint is public in Compose for convenience and should be restricted to the
internal network in a real deployment. Label values come only from fixed sets
(status, failure code, error kind, SQLSTATE, reason); identifiers are never
labels.

| Requirement | Metrics |
| --- | --- |
| Results by status | `wager_results_total{status}`, `wager_rejections_total{failure_code}`, `wager_errors_total{kind}` (`invalid_request`, `idempotency_conflict`, `external_transaction_conflict`, `wallet_not_found`, `infrastructure`), counted once for HTTP and SQS in `WageringService` |
| Duplicates | `idempotency_replays_total` (both channels), `inbox_duplicates_total` (SQS redeliveries) |
| Retries | `sqs_retries_total`, `sqs_messages_retried_total`, `outbox_retries_total`, `outbox_reschedules_total`, `reference_worker_failures_total` |
| DLQ | `sqs_messages_dlq_eligible_total` (a delivery failed at the last allowed attempt, so the redrive will move it); `sqs_messages_released_total`; `sqs_message_failures_total{kind}` per failed delivery, with `kind` `invalid_message` (undecodable or invalid message, invalid request, unknown wallet), `conflict` (idempotency or external ID conflict) or `infrastructure`, the same value as `result` in the `consumer_message_retry_scheduled` log line |
| Concurrency conflicts | `wallet_lock_wait_seconds` (time to acquire the wallet lock), `wallet_lock_contended_total` (waits of 5 ms or more, an approximation of "another writer held the wallet"), `db_concurrency_conflicts_total{sqlstate}` (deadlocks and serialization failures) |
| Outbox delay | `outbox_lag_seconds` (commit to publication, per published event); gauges `outbox_pending_events` and `outbox_oldest_pending_seconds`, refreshed every publisher cycle, which keep growing when publication is stuck |
| Processing latency | `wager_processing_duration_seconds` (both channels), `http_request_duration_seconds` |
| Reconciliation | `reconciliation_total`, `reconciliation_divergences_total` |
| Health and security | `readiness_check_failures_total`, `http_dependency_unavailable_total`, `auth_failures_total{reason}`, `http_requests_total` and status classes |

## 14. Database migrations

Changes are versioned in embedded `up` and `down` SQL files (list and commands in
`README.md`). Every replica applies pending versions on startup under a
PostgreSQL advisory lock, up to the highest embedded version; a gap in the
sequence stops the start. Each version runs, with its `schema_migrations` row,
in its own transaction. DDL is transactional in PostgreSQL, so a failing
migration leaves no partial schema. `cmd/migrate` (also `/app/migrate` in the
image) applies or reverts versions with the same lock. Tests cover concurrent
`up` from two runners, revert of the latest versions, the command against an
isolated schema, and a full round trip with existing data: revert to `000001`,
apply again, and the data is kept and the later protections come back.

## 15. Verification of concurrency and failure scenarios

| Scenario | Evidence |
| --- | --- |
| Same bet 50 times in parallel | `TestProcessTransactionFiftyConcurrentDuplicateBets`; E2E `TestE2EFiftyParallelIdenticalBets` through Nginx and three replicas: one debit, 49 replays |
| Two 80.00 bets on 100.00 | `TestProcessTransactionConcurrentBetsLockOneWallet` (plus resends); E2E `TestE2EConcurrentOverdraftBets`: one processed, one `insufficient_funds` (422), balance 20.00, one debit, resends unchanged |
| Different wallets in parallel | `TestProcessTransactionDifferentWalletsDoNotWaitForEachOther`; E2E `TestE2EIndependentWalletsInParallel` |
| Three independent processes | the E2E suite runs against three Compose replicas |
| Consumer stops after commit, before delete | `TestFinancialQueueConsumerRecoveryAfterCommit`; E2E `TestE2EInstanceFailuresDuringSQSLoad` freezes a replica holding an undeleted message and kills it with SIGKILL. Across runs it caught interruptions both after the commit (the Inbox stopped the redelivery) and before it (a survivor applied it) |
| SIGTERM during load | same E2E test: the stopped replica leaves no received message unfinished; unit tests cover completing the batch and releasing visibility at the deadline |
| Repeated delivery | `TestFinancialQueueConsumerAppliesRepeatedDeliveryOnce`: two real SQS deliveries of one `messageId`, one effect, Inbox duplicate counted |
| Same operation via HTTP and SQS | E2E `TestE2ESameOperationThroughHTTPAndSQS`: concurrent HTTP and SQS plus a later SQS copy with another `messageId`, no conflict, one debit |
| Conflicting payload via SQS | `TestFinancialQueueConsumerSendsIdempotencyConflictToDLQ` |
| Two publishers; publish then crash before mark | `TestTwoOutboxPublishersPublishConcurrentEventsOnce`, `TestOutboxPublisherRecoversAfterPublishBeforeMarkPublished` (same `eventId` on republication) |
| Reversal before its reference; expiry | `TestReferenceWorkersResolveReversalRegisteredByStoppedInstance`, `TestReferenceWorkerRejectsExpiredReferenceWithObservedBalance` |
| Restart | E2E `TestE2ERestartPreservesIdempotency`: after restarting every replica, replays return the original result and a pending reference is still pending |
| Temporary PostgreSQL or SQS outage | manual runs (section 11); `TestHealthReadyWithRealDependencies`; `TestOutboxPublisherExposesBacklogWhilePublicationIsStuck` |
| Ledger vs stored balance | every E2E wallet is checked through the reconciliation endpoint |

## 16. Interpretations, limitations and work not done

- **`FAILED` is never persisted.** Infrastructure failures are not stored as
  terminal rows. Transient ones roll back and are retried (`503` on HTTP, SQS
  redelivery). Permanent ones on SQS stay in the DLQ, unapplied and replayable;
  on HTTP they answer `500` with nothing stored. Writing a `FAILED` row would need
  the database that is often the failing dependency, and it would turn a
  recoverable situation into a terminal one. The state exists in the domain and
  the schema, and a reversal whose reference is `FAILED` is rejected as
  `reference_incompatible`.
- **Broker policies** are defined, versioned and applied to the queues (section
  10), but LocalStack Community does not enforce IAM. A request from a principal
  outside the policy is therefore not actually denied locally; on AWS it would be.
  The consumer trusts `data.providerId` as coming from an authenticated producer:
  the SQS channel is internal and trusted, and binding the provider to the
  sender is the production evolution described in section 10.
- **Ledger and privileged database roles.** The triggers stop `UPDATE`,
  `DELETE` and `TRUNCATE` from any client, but a superuser or the table owner
  can still disable or drop them. The application connects as `postgres`
  because every replica applies the migrations at startup, and applying them
  needs the owner's privileges. A separate runtime role without superuser and
  with `REVOKE UPDATE, DELETE, TRUNCATE ON wallet_ledger_entries` would protect
  little while the same process also holds the owner's credentials. Doing it
  properly means moving migrations out of the replicas' startup: a one-shot job
  (`cmd/migrate`) connects as the owner, and the replicas connect as a role
  with `SELECT, INSERT` on the ledger and no DDL. That was not done here, so
  that `docker compose up` keeps applying the schema from any replica.
- **`WIN` reference.** A `WIN` may carry `referenceExternalTransactionId` (the
  challenge makes it optional). It is stored and returned in reads and events,
  but it is not resolved or validated against a bet; only `REFUND` and
  `ROLLBACK` require and resolve a reference.
- **Reading another provider's transaction answers `403`,** which reveals that
  the ID exists. The challenge does not require `404`.
- **Currencies** are validated by format (three upper-case letters), not
  against the ISO 4217 list. Flows were exercised in BRL; currency mismatches
  are tested.
- **Payload hash change.** The hash moved to canonical JSON with sorted keys
  during development. Operations stored before that change would answer `409`
  if replayed with the same key. Only test data existed.
- **Fixed policies.** The reference retry policy (1 min base, 5 retries), the
  poll intervals and the consumer settings are constants, not environment
  variables. `OIDC_AUDIENCE` must be set (Compose sets `backend-api`): when it
  is empty, go-oidc has no client ID to check and refuses every token, so all
  protected routes answer `401`
  (`TestVerifierWithoutAudienceRejectsEveryToken`).
- **Metrics** are per process and `/metrics` is public in Compose.
  `wallet_lock_contended_total` uses a 5 ms threshold as an approximation.
- **Reference worker.** A pending row whose processing failed permanently (only
  possible with corrupted data) would be retried every second and, being first
  in order, would delay the others. No current code path produces such a row.
- **Schema versions.** A binary does not refuse a database with newer
  migrations than it embeds.
- **Latency after replica changes.** A message delivered to a long poll of a
  replica that just stopped waits for the 30 s visibility timeout (section 8).
- **Not implemented** (optional in the challenge): double-entry ledger,
  OpenTelemetry tracing, dashboards, load tests.
