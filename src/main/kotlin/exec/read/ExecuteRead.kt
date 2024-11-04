package dynq.exec.read

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.common.base.CharMatcher
import dynq.cli.command.ReadCommand
import dynq.error.FriendlyError
import dynq.exec.read.model.FilterOutput
import dynq.exec.read.model.RawReadOutput
import dynq.exec.read.model.ReadMetadata
import dynq.jq.jq
import dynq.jq.jqn
import dynq.model.KeyMatcher
import dynq.model.ddb.DynamoDbItem
import dynq.model.ddb.PaginatedResponse
import dynq.model.ddb.ReadProjection
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
            filter(ddb, command, readChannel, filterChannel)
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
                PaginatedResponse.from(response)
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

private suspend fun filter(
    ddb: DynamoDbClient,
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
    var keys: Pair<String, String?>? = null
    val batch = mutableListOf<RawReadOutput>()

    for (input in readChannel) {
        batch.add(
            if (command.expand()) {
                expandItems(
                    ddb,
                    command,
                    input,
                    keys ?: getTableKeys(ddb, command.tableName()).also { keys = it }
                )
            } else input
        )
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
        .totalSegments(command.concurrency())
        .consistentRead(command.consistentRead())
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .apply { sanitizeProjExpr(command.projectionExpression()).applyTo(this) }
}

private suspend fun <T> parallelize(
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
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .indexName(command.globalIndexName())
        .consistentRead(command.consistentRead())
        .apply { sanitizeProjExpr(command.projectionExpression(), exprAttrNames).applyTo(this) }
}

private fun buildGetItem(
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
        ).apply { sanitizeProjExpr(command.projectionExpression()).applyTo(this) }
        .build()
}

private fun sanitizeProjExpr(
    projExpr: String?,
    exprAttrNames: Map<String, String>? = null
): ReadProjection {
    if (projExpr == null) {
        return ReadProjection(null, exprAttrNames)
    }
    val names = projExpr.split(",").map(String::trim)
    val aliases = mutableListOf<String>()
    val map = mutableMapOf<String, String>()
    if (exprAttrNames != null) {
        map.putAll(exprAttrNames)
    }
    for (i in names.indices) {
        val alias = "#a$i"
        map[alias] = names[i]
        aliases.add(alias)
    }
    return ReadProjection(aliases.joinToString(", "), map)
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

private fun expandItems(
    ddb: DynamoDbClient,
    command: ReadCommand,
    readOutput: RawReadOutput,
    keyNames: Pair<String, String?>
): RawReadOutput {
    var unprocessedKeys: KeysAndAttributes? = null
    val expanded = mutableListOf<RawReadOutput>()

    // TODO parallelize
    for (chunk in readOutput.items.chunked(100)) do {
        unprocessedKeys = unprocessedKeys ?: KeysAndAttributes.builder()
            .keys(chunk.map { item ->
                item.filterKeys { keyNames.toList().contains(it) }
            })
            .consistentRead(command.consistentRead())
            .apply { sanitizeProjExpr(command.projectionExpression()).applyTo(this) }
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

fun RawReadOutput.add(other: RawReadOutput): RawReadOutput {
    return this.copy(
        items = this.items + other.items,
        meta = mergeReadMetadata(this.meta, other.meta)
    )
}

fun ReadMetadata.add(other: ReadMetadata): ReadMetadata {
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

fun mergeReadMetadata(
    primary: ReadMetadata,
    secondary: ReadMetadata
): ReadMetadata {
    fun addNullableInts(n1: Int?, n2: Int?): Int? {
        return listOfNotNull(n1, n2).takeUnless { it.isEmpty() }?.sum()
    }
    listOfNotNull(primary.scannedCount, secondary.scannedCount).takeUnless { it.isEmpty() }?.sum()
    return primary.copy(
        consumedCapacity = primary.consumedCapacity + secondary.consumedCapacity,
        requestCount = primary.requestCount + secondary.requestCount,
        scannedCount = addNullableInts(primary.scannedCount, secondary.scannedCount),
        hitCount = addNullableInts(primary.hitCount, secondary.hitCount)
    )
}

private fun getTableKeys(
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