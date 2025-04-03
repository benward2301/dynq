FROM amazon/dynamodb-local:latest

WORKDIR /home/dynamodblocal

ADD data.tar.gz .

EXPOSE 8000

USER root

CMD ["-jar", "DynamoDBLocal.jar", "-sharedDb", "-dbPath", "./data"]
