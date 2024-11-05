package dynq.exec.read

import dynq.cli.command.ReadCommand
import dynq.error.FriendlyError
import dynq.executor.read.fn.aggregateMetadata
import dynq.executor.read.fn.expandItems
import dynq.executor.read.model.FilterOutput
import dynq.executor.read.model.RawReadOutput
import dynq.jq.jqn
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
        batch.add(
            if (command.expand()) {
                expandItems(ddb, command, input)
            } else input
        )
        if (batch.size == input.batchSize) {
            val output = filterBatch(batch, expression)
            outputChannel.send(output)
            hitCount += output.items.size
            batch.clear()

            if (limit != null && limit <= hitCount || isMaxHeapSizeExceeded(command)) {
                break
            }
        }

    }

    if (batch.isNotEmpty()) {
        outputChannel.send(filterBatch(batch, expression))
    }
    outputChannel.close()
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

private fun isMaxHeapSizeExceeded(command: ReadCommand): Boolean {
    val runtime = Runtime.getRuntime()
    val defaultMax = runtime.maxMemory() * 0.8
    val effectiveMax = command.maxHeapSize()
        ?.times(1e6)
        ?.coerceAtMost(defaultMax)
        ?: defaultMax
    return effectiveMax <= runtime.totalMemory() - runtime.freeMemory()
}