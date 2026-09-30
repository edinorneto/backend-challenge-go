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

## Outbox publisher

This version implements the transactional outbox and the publisher worker. Every
financial mutation still writes the wallet and wager data and inserts the
corresponding outbox event in the same PostgreSQL transaction. The publish step
runs outside the financial transaction in a dedicated worker.

The worker claims a small pending batch with PostgreSQL lock-skipping semantics,
uses the `aggregate_id` as the FIFO `MessageGroupId`, and reuses the stable
`event_id` as `MessageDeduplicationId` so retries remain idempotent at the
message layer. On failures, it schedules a retry with exponential backoff.

The `wager-transactions.fifo` queue is the input command queue. Its messages
have type `WagerTransactionRequested` and are processed by the same financial
use case used by HTTP. The Outbox Publisher publishes the resulting events to
the separate `wager-events.fifo` output queue. DLQ and reconciliation remain
outside this stage.

The transaction queue consumer is lifecycle-managed. It uses long polling,
processes independent FIFO message groups in parallel, records durable Inbox
state keyed by `consumer_name` and `message_id`, executes the shared financial
use case, and commits Inbox, wallet, wager transaction, ledger, and Outbox
changes in one PostgreSQL transaction. It deletes a message only after that
commit. Processing failures leave the message for SQS redelivery and the
configured redrive policy.
