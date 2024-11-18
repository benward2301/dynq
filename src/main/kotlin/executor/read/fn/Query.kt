package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.ddb.model.PaginatedResponse
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
    partitionKey: KeyMatcher.Discrete,
    sortKey: KeyMatcher?
) {
    parallelize(
        command,
        buildQueries(command, partitionKey, sortKey),
    ) { builder ->
        autoPaginate(
            command,
            readChannel,
            "Query",
        ) { startKey, remaining ->
            val response = ddb.query(
                builder.exclusiveStartKey(startKey)
                    .limit(remaining)
                    .build()
            )
            PaginatedResponse.from(response)
        }
    }
}

private fun buildQueries(
    command: ReadCommand,
    partitionKey: KeyMatcher.Discrete,
    sortKey: KeyMatcher?
): List<QueryRequest.Builder> {
    return partitionKey.values.flatMap { partitionKeyValue ->
        if (sortKey is KeyMatcher.Discrete) {
            sortKey.values.map { sortKeyValue ->
                buildSingleItemQuery(
                    command,
                    partitionKey.name,
                    partitionKeyValue,
                    sortKey.name,
                    sortKeyValue
                )
            }
        } else {
            listOf(
                buildMultiItemQuery(
                    command,
                    partitionKey.name,
                    partitionKeyValue,
                    sortKey as KeyMatcher.Continuous?
                )
            )
        }
    }
}

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