package dynq.executor.read.fn

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.common.base.CharMatcher
import dynq.cli.command.ReadCommand
import dynq.cli.logging.*
import dynq.executor.read.model.FilterOutput
import dynq.executor.read.model.ReadMetadata
import dynq.jq.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import software.amazon.awssdk.protocols.jsoncore.JsonNode

private const val METADATA_PROP = "meta"
private const val CONTENT_PROP = "content"

suspend fun collate(
    command: ReadCommand,
    outputChannel: Channel<FilterOutput>,
) {
    if (command.stream()) {
        streamOutput(command, outputChannel)
        return
    }

    val output = collectOutput(command, outputChannel)

    LogEntry.transform { it?.replace("$SPINNER", style(GREEN)("$CHECK_MARK")) }

    CharMatcher.anyOf("\r\n\t").removeFrom(
        output.items.toString()
    ).let {
        log { "Aggregating results" }
        jq(
            it,
            filter = buildAggregationFilter(command, output.meta),
            sortKeys = command.rearrangeKeys(),
            onError = throwJqError("bad aggregate filter")
        )
    }.let {
        jq(
            it,
            filter = buildPresentationFilter(command, output),
            pretty = !command.compact(),
            colorize = command.colorize()
        )
    }.also { LogEntry.close() }
        .let(::println)
}

private suspend fun streamOutput(
    command: ReadCommand,
    outputChannel: Channel<FilterOutput>
) {
    var remaining = command.limit()

    for (batch in outputChannel) {
        val found = batch.items
            .takeIf { it.isNotEmpty() }
            .also {
                if (it != null) {
                    jq(
                        it.toString(),
                        filter = ".[0:${remaining ?: ""}] | .[]",
                        pretty = !command.compact(),
                        sortKeys = command.rearrangeKeys(),
                        colorize = command.colorize()
                    ).let(::println)
                }
            }.let { it?.size ?: 0 }

        if (remaining != null) {
            remaining -= found
            if (remaining <= 0) break
        }
    }
}

private suspend fun collectOutput(
    command: ReadCommand,
    outputChannel: Channel<FilterOutput>
): FilterOutput {
    val prune = command.prune()
    val metas = mutableListOf<ReadMetadata>()
    var items = mutableListOf<JsonNode>()
    var aggregatorTested = false

    outputChannel.consumeEach { output ->
        if (!aggregatorTested && items.isNotEmpty()) {
            testAggregationFilter(command, output)
            aggregatorTested = true
        }
        items.addAll(output.items)
        if (prune != null) {
            items = jqn(
                items.toString(),
                prune,
                onError = throwJqError("bad prune filter")
            ).also {
                if (!it.isArray) {
                    throw Exception("the output of the prune filter must be an array")
                }
            }.asArray()
        }
        metas.add(output.meta)
    }
    return FilterOutput(
        items,
        aggregateMetadata(
            metas,
            command.limit() == null && command.concurrency() == 1
        )
    )
}

private fun buildAggregationFilter(
    command: ReadCommand,
    metadata: ReadMetadata
): String {
    return ".[0]".takeUnless { command.reduce() == null }
        .pipeToNullable(metadata.hitCount?.let { "$it as \$count" })
        .pipeToNullable(command.aggregate())
        .pipeToNonNull(".")
}

private fun testAggregationFilter(
    command: ReadCommand,
    batch: FilterOutput,
) {
    jq(
        batch.items.toString(),
        buildAggregationFilter(command, batch.meta),
        onError = {
            warn { wrapJqError("A dry aggregation run produced the following error(s):")(it).message!! }
        }
    )
}

private fun buildPresentationFilter(
    command: ReadCommand,
    output: FilterOutput
): String? {
    if (command.contentOnly()) {
        return null
    }
    val metadata = jacksonObjectMapper().writer().writeValueAsString(output.meta)
        .pipe("del(..|nulls)")

    if (command.metadataOnly()) {
        return metadata
    }
    return "{$METADATA_PROP: ($metadata), $CONTENT_PROP: .}"
}
