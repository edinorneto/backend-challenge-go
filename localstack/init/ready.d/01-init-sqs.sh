#!/bin/sh
set -eu

QUEUE_NAME="${SQS_TRANSACTION_QUEUE:-wager-transactions.fifo}"
DLQ_NAME="${SQS_TRANSACTION_DLQ:-wager-transactions-dlq.fifo}"
MAX_RECEIVE_COUNT="${SQS_MAX_RECEIVE_COUNT:-5}"

awslocal sqs create-queue \
  --queue-name "$DLQ_NAME" \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  --query QueueUrl \
  --output text

REDRIVE_POLICY="$(printf '{"deadLetterTargetArn":"arn:aws:sqs:%s:000000000000:%s","maxReceiveCount":"%s"}' \
  "${AWS_DEFAULT_REGION:-us-east-1}" "$DLQ_NAME" "$MAX_RECEIVE_COUNT")"
ATTRIBUTES_FILE="/tmp/transaction-queue-attributes.json"
ESCAPED_REDRIVE_POLICY="$(printf '%s' "$REDRIVE_POLICY" | sed 's/"/\\"/g')"
printf '{"FifoQueue":"true","ContentBasedDeduplication":"true","RedrivePolicy":%s}' \
  "\"$ESCAPED_REDRIVE_POLICY\"" > "$ATTRIBUTES_FILE"

awslocal sqs create-queue \
  --queue-name "$QUEUE_NAME" \
  --attributes "file://$ATTRIBUTES_FILE" \
  --query QueueUrl \
  --output text

echo "Created SQS queues: $QUEUE_NAME and $DLQ_NAME"
