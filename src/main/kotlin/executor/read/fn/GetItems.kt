package dynq.executor.read.fn

import cli.logging.fmt.formatKey
import cli.logging.fmt.formatProgressMessage
import cli.logging.fmt.formatRequestOp
import dynq.cli.command.ReadCommand
import dynq.cli.logging.*
import dynq.ddb.model.Key
import dynq.ddb.model.KeyMember
import dynq.executor.read.model.KeyMatcher
import dynq.executor.read.model.RawReadOutput
import dynq.executor.read.model.ReadMetadata
import kotlinx.coroutines.channels.Channel
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity

suspend fun getItems(
    command: ReadCommand,
    readChannel: Channel<RawReadOutput>,
    ddb: DynamoDbClient,
    partitionKey: KeyMatcher.Discrete,
    sortKey: KeyMatcher.Discrete
) {
    LogEntry.new(pos = 0).log { "${formatRequestOp("GET")} ${command.tableName()}" }

    val keys = partitionKey.values.flatMap { partitionKeyValue ->
        sortKey.values.map { sortKeyValue ->
            Key(
                KeyMember(partitionKey.name, partitionKeyValue),
                KeyMember(sortKey.name, sortKeyValue)
            )
        }
    }
    parallelize(
        command.concurrency(),
        keys,
    ) { key ->
        val response = ddb.getItem(buildGetItemRequest(command, key))
        val items = if (response.hasItem())
            listOf(response.item()) else
            emptyList()

        readChannel.send(
            RawReadOutput(
                items,
                ReadMetadata(response.consumedCapacity().capacityUnits())
            )
        )
        LogEntry.new(indent = 1, pos = 1).log {
            val icon = if (items.isEmpty())
                style(YELLOW)("$EM_DASH") else
                style(GREEN)("$CHECK_MARK")
            formatProgressMessage(icon, null, formatKey(key))
        }
    }
}

private fun buildGetItemRequest(
    command: ReadCommand,
    key: Key
): GetItemRequest {

    return GetItemRequest.builder()
        .tableName(command.tableName())
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .consistentRead(command.consistentRead())
        .key(
            mutableMapOf(
                Pair(key.partition.name, key.partition.value)
            ).also {
                if (key.sort != null) {
                    it[key.sort.name] = key.sort.value
                }
            }
        ).apply { sanitizeProjectionExpression(command.projectionExpression()).applyTo(this) }
        .build()
}
