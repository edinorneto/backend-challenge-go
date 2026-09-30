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

The PostgreSQL outbox is already persisted transactionally by the application.
SQS connectivity and queue discovery are prepared here; the outbox publisher,
SQS consumer, and inbox processing are intentionally not implemented yet.
