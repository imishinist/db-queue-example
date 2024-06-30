#!/bin/bash

endpoint="http://localhost:9324"
queue_url=$endpoint/000000000000/default

while true; do
  for message in $(aws sqs receive-message --queue-url $queue_url \
    --endpoint-url $endpoint \
    --max-number-of-messages 10 \
    --wait-time-seconds 20 | jq -rcM '.Messages[]'); do
    
    body=$(echo "$message" | jq -r '.Body')
    receipt_handle=$(echo "$message" | jq -r '.ReceiptHandle')

    # processing
    user_id=$(echo "$body" | jq -r '.user_id')
    echo "$user_id" >> /tmp/next_user_ids
    aws sqs delete-message --queue-url $queue_url \
      --receipt-handle "$receipt_handle" \
      --endpoint-url $endpoint
    echo "delete message: $receipt_handle"
  done
done
