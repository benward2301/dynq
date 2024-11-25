#!/bin/bash

docker rm -f dynamodb-local-dvdr &> /dev/null

docker run -d \
  -u root \
  -p 8000:8000 \
  -v "$(pwd)/docker/dvd-rental/volume:/home/dynamodblocal/data" \
  -w /home/dynamodblocal \
  --name dynamodb-local-dvdr \
  amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -sharedDb -dbPath ./data
