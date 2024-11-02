package dynq.exec.read

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.common.base.CharMatcher
import dynq.cli.command.ReadCommand
import dynq.error.FriendlyError
import dynq.jq.jq
import dynq.jq.jqn
import dynq.model.DynamoDbItem
import dynq.model.KeyMatcher
import dynq.model.PaginatedResponse
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.toList
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.JsonItemAttributeConverter
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.AwsProfileRegionProvider
import software.amazon.awssdk.regions.providers.AwsRegionProvider
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger

private const val PARTITION_KEY_NAME_TOKEN = "#P"
private const val PARTITION_KEY_VALUE_TOKEN = ":p"
private const val SORT_KEY_NAME_TOKEN = "#S"
private const val SORT_KEY_VALUE_TOKEN = ":s"
private const val SORT_KEY_AUX_VALUE_TOKEN = ":t"

private const val METADATA_PROP = "meta"
private const val CONTENT_PROP = "content"

fun executeRead(command: ReadCommand) {
    val ddb = createDDbClient(command)
    val readChannel = Channel<RawReadOutput>(command.concurrency())
    val filterChannel = Channel<FilterOutput>(Channel.UNLIMITED)
    val partitionKey = buildPartitionKeyMatcher(command.partitionKey())
    val sortKey = buildKeyMatcher(command.sortKey())

    runBlocking {
        val reading = launch(Dispatchers.IO) {
            when {
                partitionKey == null ->
                    scan(command, readChannel, ddb)

                sortKey is KeyMatcher.Values && command.globalIndexName() == null ->
                    getItem(command, readChannel, ddb, partitionKey, sortKey)

                else ->
                    query(command, readChannel, ddb, partitionKey, sortKey)
            }
            readChannel.close()
        }
        launch {
            filter(command, readChannel, filterChannel)
            reading.cancel()
        }
        launch {
            present(command, filterChannel)
        }
    }
}

private fun createDDbClient(command: ReadCommand): DynamoDbClient {
    val endpointUrl = command.endpointUrl()
    val profile = command.profile()

    val regionProvider: AwsRegionProvider
    val credentialsProvider: AwsCredentialsProvider

    if (profile != null) {
        regionProvider = AwsProfileRegionProvider(null, profile)
        credentialsProvider = ProfileCredentialsProvider.create(profile)
    } else {
        regionProvider = DefaultAwsRegionProviderChain()
        credentialsProvider = DefaultCredentialsProvider.create()
    }

    val region = Region.of(command.region() ?: regionProvider.region.id())

    val builder = DynamoDbClient.builder()
        .region(region)
        .credentialsProvider(credentialsProvider)

    if (endpointUrl != null) {
        builder.endpointOverride(URI.create(endpointUrl))
    }

    return builder.build()
}

private suspend fun scan(
    command: ReadCommand,
    readChannel: Channel<RawReadOutput>,
    ddb: DynamoDbClient,
) = coroutineScope {
    val scannedCount = AtomicInteger()

    for (segment in 0..<command.concurrency()) {
        launch {
            autoPaginate(
                command,
                readChannel,
                "Scan"
            ) { startKey, remaining ->
                val request = buildScanBase(command)
                    .segment(segment)
                    .exclusiveStartKey(startKey)
                    .limit(remaining)
                    .build()
                val response = ddb.scan(request)
                scannedCount.addAndGet(response.scannedCount())
                PaginatedResponse(
                    response.items(),
                    response.consumedCapacity().capacityUnits(),
                    response.scannedCount(),
                    if (response.hasLastEvaluatedKey()) response.lastEvaluatedKey() else null
                )
            }
        }
    }
}

private suspend fun query(
    command: ReadCommand,
    readChannel: Channel<RawReadOutput>,
    ddb: DynamoDbClient,
    partitionKey: KeyMatcher.Values,
    sortKey: KeyMatcher?
) {
    parallelise(
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
            PaginatedResponse(
                response.items(),
                response.consumedCapacity().capacityUnits(),
                response.scannedCount(),
                if (response.hasLastEvaluatedKey()) response.lastEvaluatedKey() else null
            )
        }
    }
}

private suspend fun getItem(
    command: ReadCommand,
    readChannel: Channel<RawReadOutput>,
    ddb: DynamoDbClient,
    partitionKey: KeyMatcher.Values,
    sortKey: KeyMatcher.Values
) {
    val requests = partitionKey.values.flatMap { partitionKeyValue ->
        sortKey.values.map { sortKeyValue ->
            buildGetItem(
                command,
                partitionKey.name,
                partitionKeyValue,
                sortKey.name,
                sortKeyValue
            )
        }
    }
    parallelise(
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

private suspend fun filter(
    command: ReadCommand,
    readChannel: Channel<RawReadOutput>,
    filterChannel: Channel<FilterOutput>
) {
    val limit = command.limit()
    val pretransform = command.pretransform()
    val where = command.where()
    val transformer = command.transform()

    var expression = "[.[]"
    if (pretransform != null) {
        expression += " | $pretransform"
    }
    if (where != null) {
        expression += " | select($where)"
    }
    if (transformer != null) {
        expression += " | $transformer"
    }
    expression += "]"

    var hitCount = 0
    val batch = mutableListOf<RawReadOutput>()

    for (input in readChannel) {
        batch.add(input)
        if (batch.size == input.batchSize) {
            val output = filterBatch(batch, expression)
            filterChannel.send(output)
            hitCount += output.items.size
            batch.clear()

            if (limit != null && limit <= hitCount || isMaxHeapSizeExceeded(command)) {
                break
            }
        }

    }

    if (batch.isNotEmpty()) {
        filterChannel.send(filterBatch(batch, expression))
    }
    filterChannel.close()
}

private suspend fun present(
    command: ReadCommand,
    filterChannel: Channel<FilterOutput>
) {
    val limit = command.limit()
    val aggregator = command.aggregate()
    val contentOnly = command.contentOnly()
    val resultSets = filterChannel.toList()

    var expression = "flatten | .[0:${limit ?: ""}]"
    if (aggregator != null) {
        expression += " | $aggregator"
    }

    if (!contentOnly) {
        val metadata = aggregateMetadata(
            resultSets.map { it.meta },
            limit == null && command.concurrency() == 1
        )
        expression += " | {$METADATA_PROP: ${
            jacksonObjectMapper().writer().writeValueAsString(metadata)
        }, $CONTENT_PROP: .} | del(..|nulls)"
    }

    var content = CharMatcher.anyOf("\r\n\t").removeFrom(
        resultSets.map { it.items }
            .flatten()
            .toString()
    )
    if (command.rearrangeAttributes()) {
        content = jq(content, sortKeys = true)!!
    }

    val output = jq(
        content,
        filter = expression,
        pretty = !command.compact(),
        colorize = command.colorize() && !command.monochrome()
    )
    if (output == null) {
        throw FriendlyError("bad aggregation filter")
    }

    println(output)
}

private fun aggregateMetadata(
    entries: List<ReadMetadata>,
    includeLastEvaluatedKey: Boolean
): ReadMetadata {
    var requestCount = 0
    var consumedCapacity = 0.0
    var scannedCount: Int? = null
    var hitCount: Int? = null

    for (metadata in entries) {
        requestCount += metadata.requestCount
        consumedCapacity += metadata.consumedCapacity
        if (metadata.scannedCount != null) {
            scannedCount = (scannedCount ?: 0) + metadata.scannedCount
        }
        if (metadata.hitCount != null) {
            hitCount = (hitCount ?: 0) + metadata.hitCount
        }
    }

    val last = entries.last()

    return ReadMetadata(
        last.requestType,
        consumedCapacity,
        requestCount = requestCount,
        scannedCount = scannedCount,
        hitCount = hitCount,
        lastEvaluatedKey = if (includeLastEvaluatedKey) last.lastEvaluatedKey else null
    )
}

private fun filterBatch(
    batch: List<RawReadOutput>,
    expression: String
): FilterOutput {
    val items = jqn(
        batch.flatMap { it.items }
            .map(EnhancedDocument::fromAttributeValueMap)
            .map(EnhancedDocument::toJson)
            .toString(),
        expression,
    )?.asArray()

    if (items == null) {
        throw FriendlyError("bad item filter")
    }

    val metadata = aggregateMetadata(batch.map { it.meta }, true)
        .copy(hitCount = items.size)

    return FilterOutput(
        items,
        metadata
    )
}

private suspend fun autoPaginate(
    command: ReadCommand,
    channel: Channel<RawReadOutput>,
    requestType: String,
    read: (
        startKey: DynamoDbItem?,
        remaining: Int?
    ) -> PaginatedResponse
) {
    var remaining = command.scanLimit()
    var startKey = command.startKey()?.let {
        EnhancedDocument.fromJson(
            jq(input = it)
        ).toMap()
    }
    do {
        val response = read(startKey, remaining)
        channel.send(
            RawReadOutput(
                response.items,
                ReadMetadata(
                    requestType,
                    response.consumedCapacity,
                    scannedCount = response.scannedCount,
                    lastEvaluatedKey = response.lastEvaluatedKey
                ),
                command.concurrency()
            )
        )
        if (remaining != null) {
            remaining -= response.scannedCount
        }
        startKey = response.lastEvaluatedKey
    } while (startKey != null && (remaining ?: 1) > 0)
}

private fun buildScanBase(command: ReadCommand): ScanRequest.Builder {
    return ScanRequest.builder()
        .tableName(command.tableName())
        .projectionExpression(command.projectionExpression())
        .totalSegments(command.concurrency())
        .consistentRead(command.consistentRead())
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
}

private suspend fun <T> parallelise(
    command: ReadCommand,
    inputs: Collection<T>,
    consume: suspend (item: T) -> Unit
) = coroutineScope {
    val channel = Channel<T>(Channel.UNLIMITED)
    inputs.forEach { channel.send(it) }
    channel.close()

    repeat(command.concurrency().coerceAtMost(inputs.size)) {
        launch {
            for (input in channel) {
                consume(input)
            }
        }
    }
}

private fun buildQueries(
    command: ReadCommand,
    partitionKey: KeyMatcher.Values,
    sortKey: KeyMatcher?
): List<QueryRequest.Builder> {
    return partitionKey.values.flatMap { partitionKeyValue ->
        if (sortKey is KeyMatcher.Values) {
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
                    sortKey as KeyMatcher.Range? // TODO infer?
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
    sortKey: KeyMatcher.Range?
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
        } else if (sortKey.beg != null) {
            if (sortKey.beg.type() == AttributeValue.Type.S) {
                sortExpr = "begins_with($SORT_KEY_NAME_TOKEN, $SORT_KEY_VALUE_TOKEN)"
                sortOperand = sortKey.beg
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
        .projectionExpression(
            command.projectionExpression()?.let { sanitizeProjExpr(it, exprAttrNames) }
        )
        .expressionAttributeNames(exprAttrNames)
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .indexName(command.globalIndexName())
        .consistentRead(command.consistentRead())
}

private fun buildGetItem(
    command: ReadCommand,
    partitionKeyName: String,
    partitionKeyValue: AttributeValue,
    sortKeyName: String,
    sortKeyValue: AttributeValue
): GetItemRequest {
    val builder = GetItemRequest.builder()
        .tableName(command.tableName())
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .consistentRead(command.consistentRead())
        .key(
            mapOf(
                Pair(partitionKeyName, partitionKeyValue),
                Pair(sortKeyName, sortKeyValue),
            )
        )
    val projExpr = command.projectionExpression()
    if (projExpr != null) {
        val exprAttrNames = mutableMapOf<String, String>()
        builder.projectionExpression(sanitizeProjExpr(projExpr, exprAttrNames))
            .expressionAttributeNames(exprAttrNames)
    }
    return builder.build()
}

private fun sanitizeProjExpr(
    projExpr: String,
    exprAttrNames: MutableMap<String, String>
): String {
    val names = projExpr.split(",").map(String::trim)
    val aliases = mutableListOf<String>()
    for (i in names.indices) {
        val alias = "#a$i"
        exprAttrNames[alias] = names[i]
        aliases.add(alias)
    }
    return aliases.joinToString(", ")
}

private fun buildKeyMatcher(filter: String?): KeyMatcher? {
    if (filter == null) {
        return null
    }
    val name = jqn(
        input = "{}",
        filter = "$filter | keys[0]"
    )?.asString()
    val node = jqn(
        input = "{}",
        filter = "$filter | [.[]][0]"
    )
    if (name == null || node == null) {
        throw FriendlyError("bad key filter")
    }
    val converter = JsonItemAttributeConverter.create()

    if (node.isObject) {
        val map = converter.transformFrom(node).m()
        return KeyMatcher.Range(
            name,
            gt = map["gt"],
            gte = map["gte"],
            lt = map["lt"],
            lte = map["lte"],
            beg = map["beg"]
        )
    }

    val nodes = if (node.isArray) {
        node.asArray()
    } else if (node.isString || node.isNumber) {
        listOf(node)
    } else {
        throw Error("key value must be string or number")
    }
    return KeyMatcher.Values(name, nodes.map { converter.transformFrom(it) })
}

private fun buildPartitionKeyMatcher(filter: String?): KeyMatcher.Values? {
    if (filter == null) {
        return null
    }
    val key = buildKeyMatcher(filter)
    if (key is KeyMatcher.Values) {
        return key
    }
    throw Error("partition key values must be discrete")
}

private fun isMaxHeapSizeExceeded(command: ReadCommand): Boolean {
    val runtime = Runtime.getRuntime()
    val defaultMax = runtime.maxMemory() * 0.8
    val effectiveMax = command.maxHeapSize()
        ?.times(1e6)
        ?.coerceAtMost(defaultMax)
        ?: defaultMax
    return effectiveMax <= runtime.totalMemory() - runtime.freeMemory()
}
