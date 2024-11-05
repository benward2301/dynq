package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.ddb.model.DynamoDbItem
import dynq.executor.read.model.RawReadOutput
import dynq.executor.read.model.ReadMetadata
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity

fun expandItems(
    ddb: DynamoDbClient,
    command: ReadCommand,
    readOutput: RawReadOutput
): RawReadOutput {
    var unprocessedKeys: KeysAndAttributes? = null
    val expanded = mutableListOf<RawReadOutput>()
    val keyNames = getKeyNames(ddb, command.tableName())

    // TODO parallelize
    for (chunk in readOutput.items.chunked(100)) do {
        unprocessedKeys = unprocessedKeys ?: KeysAndAttributes.builder()
            .keys(chunk.map { item ->
                item.filterKeys { keyNames.toList().contains(it) }
            })
            .consistentRead(command.consistentRead())
            .apply { sanitizeProjectionExpression(command.projectionExpression()).applyTo(this) }
            .build()

        val requestItems = mapOf(
            Pair(
                command.tableName(),
                unprocessedKeys
            )
        )
        val response = ddb.batchGetItem(
            BatchGetItemRequest.builder()
                .requestItems(requestItems)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .build()
        )
        if (response.hasUnprocessedKeys()) {
            unprocessedKeys = response.unprocessedKeys()[command.tableName()]
        } else {
            unprocessedKeys = null
        }

        expanded.add(
            RawReadOutput(
                response.responses()[command.tableName()] as List<DynamoDbItem>,
                ReadMetadata(
                    readOutput.meta.requestType,
                    response.consumedCapacity().sumOf { it.capacityUnits() },
                )
            )
        )
    } while (unprocessedKeys != null)

    return readOutput.copy(items = emptyList())
        .add(expanded.reduce(RawReadOutput::add))
}

private fun getKeyNames(
    ddb: DynamoDbClient,
    tableName: String
): Pair<String, String?> {
    return ddb.describeTable(
        DescribeTableRequest.builder()
            .tableName(tableName)
            .build()
    ).table()
        .keySchema()
        .map { it.attributeName() }
        .let { Pair(it.first(), it.getOrNull(1)) }
}

private fun RawReadOutput.add(other: RawReadOutput): RawReadOutput {
    return this.copy(
        items = this.items + other.items,
        meta = this.meta.add(other.meta)
    )
}

private fun ReadMetadata.add(other: ReadMetadata): ReadMetadata {
    fun addNullableInts(n1: Int?, n2: Int?): Int? {
        return listOfNotNull(n1, n2).takeUnless { it.isEmpty() }?.sum()
    }
    return this.copy(
        consumedCapacity = this.consumedCapacity + other.consumedCapacity,
        requestCount = this.requestCount + other.requestCount,
        scannedCount = addNullableInts(this.scannedCount, other.scannedCount),
        hitCount = addNullableInts(this.hitCount, other.hitCount)
    )
}
