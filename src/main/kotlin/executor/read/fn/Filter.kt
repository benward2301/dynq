package dynq.executor.read.fn

import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dynq.cli.command.ReadCommand
import dynq.cli.command.option.JQ_REDUCE_ITEM_VAR
import dynq.ddb.model.DynamoDbItem
import dynq.ddb.model.RawDynamoDbItemSerializer
import dynq.executor.read.model.FilterOutput
import dynq.executor.read.model.RawReadOutput
import dynq.executor.read.model.ReadMetadata
import dynq.jq.jqn
import dynq.jq.pipe
import dynq.jq.throwJqError
import dynq.logging.*
import kotlinx.coroutines.channels.Channel
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import kotlin.math.max

suspend fun filter(
    ddb: DynamoDbClient,
    command: ReadCommand,
    readChannel: Channel<RawReadOutput>,
    outputChannel: Channel<FilterOutput>,
    terminate: suspend () -> Unit
) {
    val limit = command.limit()
    val reducer = command.reduce()
    val filter = buildItemFilter(command)

    val state = FilterState()
    val le = createLogEntry()

    if (command.logMode() == LogMode.RENDER) {
        logFilterProgress(le, state)
    }

    for (readOutput in readChannel) {
        val filterOutput = filterBatch(
            readOutput.let {
                if (command.expand()) expandItems(ddb, command, it)
                else it
            },
            filter.pipe(command.limit()?.let { ".[0:${it - state.hitCount}]" }),
            command.noUnmarshall()
        )

        state.hitCount += filterOutput.items.size
        filterOutput.meta.scannedCount?.let { state.scannedCount += it }
        if (command.logMode() == LogMode.RENDER) {
            logFilterProgress(le, state)
        }

        if (reducer == null) {
            outputChannel.send(filterOutput)
        } else {
            state.reduction = reduceBatch(filterOutput, reducer, state.reduction)
        }
        if (limit != null && limit <= state.hitCount) {
            terminate()
        }

        state.warnedLowMemory = state.warnedLowMemory || warnIfLowMemory()
    }

    logFilterProgress(le, state)
    state.done = true
    state.reduction?.let { outputChannel.send(it) }
    outputChannel.close()
}

private data class FilterState(
    var done: Boolean = false,
    var reduction: FilterOutput? = null,
    var scannedCount: Int = 0,
    var hitCount: Int = 0,
    var warnedLowMemory: Boolean = false
)

private fun filterBatch(
    batch: RawReadOutput,
    expression: String,
    noUnmarshall: Boolean
): FilterOutput {
    val itemsJson = if (noUnmarshall) {
        val mapper = jacksonObjectMapper()
        val module = SimpleModule().addSerializer(
            DynamoDbItem::class.java,
            RawDynamoDbItemSerializer()
        )
        mapper.registerModule(module)
        mapper.writeValueAsString(batch.items)
    } else {
        batch.items
            .map(EnhancedDocument::fromAttributeValueMap)
            .map(EnhancedDocument::toJson)
            .toString()
    }

    val items = jqn(
        itemsJson,
        expression,
        onError = throwJqError("bad item filter")
    ).asArray()

    return FilterOutput(
        items,
        batch.meta.copy(
            hitCount = items.size
        )
    )
}

private fun reduceBatch(
    batch: FilterOutput,
    reducer: Array<String>,
    reduction: FilterOutput?
): FilterOutput {
    val meta: ReadMetadata
    val initialValue: String

    if (reduction == null) {
        meta = batch.meta
        initialValue = reducer[0]
    } else {
        meta = aggregateMetadata(
            listOf(reduction.meta, batch.meta),
            false
        )
        initialValue = reduction.items[0].toString()
    }

    val node = jqn(
        batch.items.toString(),
        "reduce .[] as $JQ_REDUCE_ITEM_VAR ($initialValue; ${reducer[1]})",
        onError = throwJqError("bad reduce filter")
    )
    return batch.copy(
        items = listOf(node),
        meta = meta
    )
}

private fun buildItemFilter(command: ReadCommand): String {
    return "[.[]"
        .pipe(command.pretransform())
        .pipe(command.where()?.let { "select($it)" })
        .pipe(if (command.metadataOnly()) "null" else command.transform()) + "\n]"
}

private fun createLogEntry() = LogEntry.new(pos = 2)

private fun logFilterProgress(le: LogEntry?, state: FilterState, prefix: String = "") {
    le?.log { prefix + "${state.hitCount} of ${max(state.scannedCount, state.hitCount)} total item(s) retained" }
}

private fun warnIfLowMemory(): Boolean {
    val runtime = Runtime.getRuntime()
    val usedMemory = runtime.totalMemory() - runtime.freeMemory()
    val maxMemory = runtime.maxMemory()

    return (usedMemory > maxMemory * 0.6).also {
        if (it) {
            fun describeMemory(memory: Long) = (memory / 1e6).toInt()
            warn { "low memory (${describeMemory(usedMemory)} used, ${describeMemory(maxMemory)} max)" }
        }
    }
}
