package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.executor.read.model.KeyMatcher
import dynq.executor.read.model.RawReadOutput
import dynq.executor.read.model.ReadMetadata
import kotlinx.coroutines.channels.Channel
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity

suspend fun getItems(
    command: ReadCommand,
    readChannel: Channel<RawReadOutput>,
    ddb: DynamoDbClient,
    partitionKey: KeyMatcher.Discrete,
    sortKey: KeyMatcher.Discrete
) {
    val requests = partitionKey.values.flatMap { partitionKeyValue ->
        sortKey.values.map { sortKeyValue ->
            buildGetItemRequest(
                command,
                partitionKey.name,
                partitionKeyValue,
                sortKey.name,
                sortKeyValue
            )
        }
    }
    parallelize(
        command,
        requests,
    ) { request ->
        val response = ddb.getItem(request)
        readChannel.send(
            RawReadOutput(
                listOf(response.item()),
                ReadMetadata(
                    "GetItem",
                    response.consumedCapacity().capacityUnits()
                )
            )
        )
    }
}

private fun buildGetItemRequest(
    command: ReadCommand,
    partitionKeyName: String,
    partitionKeyValue: AttributeValue,
    sortKeyName: String,
    sortKeyValue: AttributeValue
): GetItemRequest {
    return GetItemRequest.builder()
        .tableName(command.tableName())
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .consistentRead(command.consistentRead())
        .key(
            mapOf(
                Pair(partitionKeyName, partitionKeyValue),
                Pair(sortKeyName, sortKeyValue),
            )
        ).apply { sanitizeProjectionExpression(command.projectionExpression()).applyTo(this) }
        .build()
}