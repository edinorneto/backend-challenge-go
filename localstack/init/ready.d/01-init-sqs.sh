#!/bin/sh
set -eu

QUEUE_NAME="${SQS_TRANSACTION_QUEUE:-wager-transactions.fifo}"
DLQ_NAME="${SQS_TRANSACTION_DLQ:-wager-transactions-dlq.fifo}"
EVENT_QUEUE_NAME="${SQS_EVENT_QUEUE:-wager-events.fifo}"
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

awslocal sqs create-queue \
  --queue-name "$EVENT_QUEUE_NAME" \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  --query QueueUrl \
  --output text

# Access policies (localstack/policies/*.json, versioned with the code). Each one
# names who may send to and consume from a queue; the placeholders are replaced
# with this account and the queue ARNs. LocalStack Community stores the
# policies but does not enforce IAM; AWS would.
ACCOUNT_ID="000000000000"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
POLICY_DIR="/etc/localstack/policies"

apply_policy() {
  queue_name="$1"
  template="$2"
  source_arn="${3:-}"
  queue_url="$(awslocal sqs get-queue-url --queue-name "$queue_name" --query QueueUrl --output text)"
  sed -e "s|__ACCOUNT__|$ACCOUNT_ID|g" \
      -e "s|__QUEUE_ARN__|arn:aws:sqs:$REGION:$ACCOUNT_ID:$queue_name|g" \
      -e "s|__SOURCE_QUEUE_ARN__|$source_arn|g" \
      "$POLICY_DIR/$template" > /tmp/policy.json
  python3 -c 'import json, sys; print(json.dumps({"Policy": json.dumps(json.load(open(sys.argv[1])))}))' \
    /tmp/policy.json > /tmp/policy-attributes.json
  awslocal sqs set-queue-attributes --queue-url "$queue_url" --attributes file:///tmp/policy-attributes.json
}

apply_policy "$QUEUE_NAME" wager-transactions.json
apply_policy "$DLQ_NAME" wager-transactions-dlq.json "arn:aws:sqs:$REGION:$ACCOUNT_ID:$QUEUE_NAME"
apply_policy "$EVENT_QUEUE_NAME" wager-events.json

echo "Created SQS queues with access policies: $QUEUE_NAME, $DLQ_NAME and $EVENT_QUEUE_NAME"
