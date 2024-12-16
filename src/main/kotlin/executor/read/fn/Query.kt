package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.cli.logging.*
import cli.logging.fmt.formatKey
import cli.logging.fmt.formatRequestOp
import dynq.ddb.model.KeyMember
import dynq.ddb.model.PaginatedResponse
import dynq.ddb.model.QueryKey
import dynq.executor.read.model.KeyMatcher
import dynq.executor.read.model.RawReadOutput
import kotlinx.coroutines.channels.Channel
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity

private const val PARTITION_KEY_NAME_TOKEN = "#P"
private const val PARTITION_KEY_VALUE_TOKEN = ":p"
private const val SORT_KEY_NAME_TOKEN = "#S"
private const val SORT_KEY_VALUE_TOKEN = ":s"
private const val SORT_KEY_AUX_VALUE_TOKEN = ":t"

suspend fun query(
    command: ReadCommand,
    readChannel: Channel<RawReadOutput>,
    ddb: DynamoDbClient,
    partitionMatcher: KeyMatcher.Discrete,
    sortMatcher: KeyMatcher?
) {
    log { "${formatRequestOp("QUERY")} ${command.tableName()}${command.globalIndexName()?.let { ".$it" } ?: ""}" }

    parallelize(
        command.concurrency(),
        buildKeys(partitionMatcher, sortMatcher),
    ) { key ->
        val ll = LogLine.new(indent = 1, pos = 1)

        val builder = buildQueryBase(
            command,
            key,
            sortMatcher
        )
        autoPaginate(
            command,
            readChannel,
            ll
        ) { startKey, limit ->
            val request = builder.exclusiveStartKey(startKey)
                .limit(limit)
                .build()
            ll.label = { formatKey(key) }

            PaginatedResponse.from(ddb.query(request))
        }
    }
}

private fun buildKeys(
    partitionMatcher: KeyMatcher.Discrete,
    sortMatcher: KeyMatcher?
): List<QueryKey> {
    return partitionMatcher.values.flatMap { partitionKeyValue ->
        val partitionKey = KeyMember(partitionMatcher.name, partitionKeyValue)
        if (sortMatcher is KeyMatcher.Discrete) {
            sortMatcher.values.map { sortKeyValue ->
                QueryKey(
                    partitionKey,
                    KeyMember(sortMatcher.name, sortKeyValue)
                )
            }
        } else {
            listOf(
                QueryKey(partitionKey, null)
            )
        }
    }
}

private fun buildQueryBase(
    command: ReadCommand,
    key: QueryKey,
    sortMatcher: KeyMatcher?
): QueryRequest.Builder {
    if (key.sort == null) {
        return buildMultiItemQuery(
            command,
            key.partition.name,
            key.partition.value,
            sortMatcher as KeyMatcher.Continuous?
        )
    } else {
        return buildSingleItemQuery(
            command,
            key.partition.name,
            key.partition.value,
            key.sort.name,
            key.sort.value
        )
    }
}

// TODO use src/main/kotlin/ddb/model/Key.kt
private fun buildSingleItemQuery(
    command: ReadCommand,
    partitionKeyName: String,
    partitionKeyValue: AttributeValue,
    sortKeyName: String,
    sortKeyValue: AttributeValue
): QueryRequest.Builder {
    return buildQueryBase(command, partitionKeyName, sortKeyName)
        .keyConditionExpression(
            "$PARTITION_KEY_NAME_TOKEN = $PARTITION_KEY_VALUE_TOKEN" +
                    " and $SORT_KEY_NAME_TOKEN = $SORT_KEY_VALUE_TOKEN"
        ).expressionAttributeValues(
            mapOf(
                Pair(PARTITION_KEY_VALUE_TOKEN, partitionKeyValue),
                Pair(SORT_KEY_VALUE_TOKEN, sortKeyValue)
            )
        )
}

// TODO use src/main/kotlin/ddb/model/Key.kt
private fun buildMultiItemQuery(
    command: ReadCommand,
    partitionKeyName: String,
    partitionKeyValue: AttributeValue,
    sortKey: KeyMatcher.Continuous?
): QueryRequest.Builder {
    val keyConditionExpr: String
    val partitionExpr = "$PARTITION_KEY_NAME_TOKEN = $PARTITION_KEY_VALUE_TOKEN"
    val exprAttrValues = mutableMapOf(Pair(PARTITION_KEY_VALUE_TOKEN, partitionKeyValue))
    if (sortKey == null) {
        keyConditionExpr = partitionExpr
    } else {
        val sortExpr: String
        val sortOperator: String
        val sortOperand: AttributeValue

        val between = sortKey.between()
        if (between != null) {
            sortExpr = "$SORT_KEY_NAME_TOKEN BETWEEN $SORT_KEY_VALUE_TOKEN AND $SORT_KEY_AUX_VALUE_TOKEN"
            sortOperand = between.first
            exprAttrValues[SORT_KEY_AUX_VALUE_TOKEN] = between.second
        } else if (sortKey.bw != null) {
            if (sortKey.bw.type() == AttributeValue.Type.S) {
                sortExpr = "begins_with($SORT_KEY_NAME_TOKEN, $SORT_KEY_VALUE_TOKEN)"
                sortOperand = sortKey.bw
            } else {
                throw Error("beg operand must be a string")
            }
        } else {
            if (sortKey.gte != null) {
                sortOperator = ">="
                sortOperand = sortKey.gte
            } else if (sortKey.gt != null) {
                sortOperator = ">"
                sortOperand = sortKey.gt
            } else if (sortKey.lt != null) {
                sortOperator = "<"
                sortOperand = sortKey.lt
            } else if (sortKey.lte != null) {
                sortOperator = "<="
                sortOperand = sortKey.lte
            } else {
                throw Error("missing sort key constraint")
            }
            sortExpr = "$SORT_KEY_NAME_TOKEN $sortOperator $SORT_KEY_VALUE_TOKEN"
        }

        keyConditionExpr = "$partitionExpr AND $sortExpr"
        exprAttrValues[SORT_KEY_VALUE_TOKEN] = sortOperand
    }

    return buildQueryBase(command, partitionKeyName, sortKey?.name)
        .keyConditionExpression(keyConditionExpr)
        .expressionAttributeValues(exprAttrValues)
        .limit(command.scanLimit())
        .consistentRead(command.consistentRead())
}

private fun buildQueryBase(
    command: ReadCommand,
    partitionKeyName: String,
    sortKeyName: String? = null
): QueryRequest.Builder {
    val exprAttrNames = mutableMapOf(Pair(PARTITION_KEY_NAME_TOKEN, partitionKeyName))
    if (sortKeyName != null) {
        exprAttrNames[SORT_KEY_NAME_TOKEN] = sortKeyName
    }
    return QueryRequest.builder()
        .tableName(command.tableName())
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .indexName(command.globalIndexName())
        .consistentRead(command.consistentRead())
        .apply { sanitizeProjectionExpression(command.projectionExpression(), exprAttrNames).applyTo(this) }
}
