package dynq.executor.read.fn

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.common.base.CharMatcher
import dynq.cli.command.ReadCommand
import dynq.cli.whisper
import dynq.executor.read.model.FilterOutput
import dynq.jq.jq
import dynq.jq.pipe
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.toList

private const val METADATA_PROP = "meta"
private const val CONTENT_PROP = "content"

suspend fun present(
    command: ReadCommand,
    outputChannel: Channel<FilterOutput>,
    cleanup: suspend () -> Unit
) {
    if (command.stream()) {
        streamOutput(command, outputChannel)
        cleanup()
        return
    }

    val filterOutputs = collectFilterOutputs(command, outputChannel)
    cleanup()

    CharMatcher.anyOf("\r\n\t").removeFrom(
        filterOutputs.map { it.items }
            .flatten()
            .toString()
    ).let {
        whisper { "Aggregating results" }
        jq(
            it,
            filter = buildAggregationFilter(command),
            sortKeys = command.rearrangeAttributes(),
            label = "aggregate"
        )
    }.let {
        jq(
            it,
            filter = buildPresentationFilter(command, filterOutputs),
            pretty = !command.compact(),
            colorize = command.colorize()
        )
    }.let(::println)
}

private suspend fun streamOutput(
    command: ReadCommand,
    outputChannel: Channel<FilterOutput>
) {
    var remaining = command.limit()

    for (filterOutput in outputChannel) {
        val found = filterOutput.items
            .takeIf { it.isNotEmpty() }
            .also {
                if (it != null) {
                    jq(
                        it.toString(),
                        filter = ".[0:${remaining ?: ""}] | .[]",
                        pretty = !command.compact(),
                        sortKeys = command.rearrangeAttributes(),
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

private suspend fun collectFilterOutputs(
    command: ReadCommand,
    outputChannel: Channel<FilterOutput>
): List<FilterOutput> {
    val filterOutputs = mutableListOf<FilterOutput>()

    outputChannel.consumeEach {
        filterOutputs.add(
            if (command.metadataOnly()) it.copy(items = listOf())
            else it
        )
    }
    return filterOutputs
}

private fun buildAggregationFilter(command: ReadCommand): String {
    return (if (command.reduce() == null) "flatten" else ".[0]")
        .pipe(command.aggregate())
}

private fun buildPresentationFilter(
    command: ReadCommand,
    filterOutputs: List<FilterOutput>
): String? {
    if (command.contentOnly()) {
        return null
    }
    val metadata = jacksonObjectMapper().writer().writeValueAsString(
        aggregateMetadata(
            filterOutputs.map { it.meta },
            command.limit() == null && command.concurrency() == 1
        )
    ).pipe("del(..|nulls)")
    if (command.metadataOnly()) {
        return metadata
    }
    return "{$METADATA_PROP: ($metadata), $CONTENT_PROP: .}"
}
