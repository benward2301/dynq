#!/bin/bash
docker run -d \
  -u root \
  -p 8000:8000 \
  -v ./dvd_rental:/home/dynamodblocal/data \
  -w /home/dynamodblocal \
  --name dynamodb-local \
  amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -sharedDb -dbPath ./data
