package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.cli.command.option.JQ_REDUCE_ITEM_VAR
import dynq.cli.logging.LogEntry
import dynq.cli.logging.log
import dynq.executor.read.model.FilterOutput
import dynq.executor.read.model.RawReadOutput
import dynq.executor.read.model.ReadMetadata
import dynq.jq.*
import kotlinx.coroutines.channels.Channel
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import kotlin.math.max

suspend fun filter(
    ddb: DynamoDbClient,
    command: ReadCommand,
    readChannel: Channel<RawReadOutput>,
    outputChannel: Channel<FilterOutput>
) {
    val limit = command.limit()
    val reducer = command.reduce()
    val filter = buildSelectionFilter(command)

    var scannedCount = 0
    var hitCount = 0
    var reduction: FilterOutput? = null

    val le = LogEntry.new(pos = 2)
    logFilterProgress(le, hitCount, scannedCount)

    for (readOutput in readChannel) {
        val filterOutput = filterBatch(
            readOutput.let {
                if (command.expand()) expandItems(ddb, command, it)
                else it
            },
            filter.pipe(command.limit()?.let { ".[0:$it]" })
        )

        hitCount += filterOutput.items.size
        filterOutput.meta.scannedCount?.let { scannedCount += it }
        logFilterProgress(le, hitCount, scannedCount)

        if (reducer == null) {
            outputChannel.send(filterOutput)
        } else {
            reduction = reduceBatch(filterOutput, reducer, reduction)
        }
        if (limit != null && limit <= hitCount) {
            break
        }
        if (isMaxHeapSizeExceeded(command)) {
            log { "Max heap size exceeded" }
            break
        }
    }

    if (reduction != null) {
        outputChannel.send(reduction)
    }
    outputChannel.close()
}

private fun filterBatch(
    batch: RawReadOutput,
    expression: String
): FilterOutput {
    val items = jqn(
        batch.items
            .map(EnhancedDocument::fromAttributeValueMap)
            .map(EnhancedDocument::toJson)
            .toString(),
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

private fun buildSelectionFilter(command: ReadCommand): String {
    return "[.[]"
        .pipe(command.pretransform())
        .pipe(command.where()?.let { "select($it)" })
        .pipe(command.transform()) + "\n]"
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

private fun logFilterProgress(le: LogEntry, hitCount: Int, scannedCount: Int) {
    le.log { "$hitCount of ${max(scannedCount, hitCount)} total item(s) retained" }
}
