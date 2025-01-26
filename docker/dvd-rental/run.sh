#!/bin/bash
set -e
dir=$(dirname "$(realpath $0)")

echo n | unzip "$dir/volume.zip" -d "$dir" &> /dev/null
docker rm -f dynamodb-local-dvdr &> /dev/null

docker run -d \
  -u root \
  -p 8000:8000 \
  -v "$dir/volume:/home/dynamodblocal/data" \
  -w /home/dynamodblocal \
  --name dynamodb-local-dvdr \
  amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -sharedDb -dbPath ./data
