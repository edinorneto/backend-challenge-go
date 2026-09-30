# Architecture

The current runtime is composed with Uber Fx:

```text
HTTP -> application -> ports -> PostgreSQL infrastructure
                                      |
                                      +-> transactional outbox

future application/workers -> SQS port -> SQS infrastructure
```

The domain packages do not depend on HTTP, PostgreSQL, AWS SDK, or LocalStack.
Financial processing remains in the existing application and PostgreSQL
repository flow. Wallet locking, ledger writes, wager transaction state, and
outbox events remain in the same PostgreSQL transaction.

This stage adds only SQS infrastructure preparation:

- AWS SDK for Go v2 SQS client;
- centralized AWS and queue configuration;
- queue URL resolution and a startup dependency check;
- LocalStack initialization for one FIFO transaction queue and one FIFO DLQ;
- Fx registration and lifecycle integration.

The outbox publisher, SQS consumer, inbox processing, retry worker, DLQ
consumer, authentication, metrics, and reconciliation are not implemented.
