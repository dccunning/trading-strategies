#!/bin/bash

SERVICE_NAME="kafka-stream.service"
EMAIL="dccunning@gmail.com"
SUBJECT="Kafka Stream Log Alert"

journalctl -fu "$SERVICE_NAME" --output=cat | while read -r line; do
  if echo "$line" | grep -Ei "warn|error"; then
    echo "$line" | mail -s "$SUBJECT" "$EMAIL"
  fi
done