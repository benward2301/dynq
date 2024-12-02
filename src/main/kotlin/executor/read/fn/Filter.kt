package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.cli.whisper
import dynq.executor.read.model.FilterOutput
import dynq.executor.read.model.RawReadOutput
import dynq.executor.read.model.ReadMetadata
import dynq.jq.jqn
import dynq.jq.pipe
import kotlinx.coroutines.channels.Channel
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

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

    for (batch in readChannel) {
        val output = filterBatch(
            batch.let {
                if (command.expand()) expandItems(ddb, command, it)
                else it
            },
            filter.pipe(command.limit()?.let { ".[0:$it]" })
        )
        hitCount += output.items.size
        output.meta.scannedCount?.let {
            scannedCount += it
            whisper { "$hitCount of $scannedCount total items retained" }
        }

        if (reducer == null) {
            outputChannel.send(output)
        } else {
            reduction = reduceBatch(output, reducer, reduction)
        }
        if (limit != null && limit <= hitCount) {
            whisper { "Item limit reached" }
            break
        }
        if (isMaxHeapSizeExceeded(command)) {
            whisper { "Max heap size exceeded" }
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
    )?.asArray()

    if (items == null) {
        throw Error("bad item filter")
    }

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
        "reduce .[] as \$item ($initialValue; ${reducer[1]})"
            .pipe(reducer.getOrNull(2))
    )
    if (node == null) {
        throw Error("bad reduce filter")
    }
    return batch.copy(
        items = listOf(node),
        meta = meta
    )
}

private fun buildSelectionFilter(command: ReadCommand): String {
    return "[.[]"
        .pipe(command.pretransform())
        .pipe(command.where()?.let { "select($it)" })
        .pipe(command.transform()) + "]"
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
