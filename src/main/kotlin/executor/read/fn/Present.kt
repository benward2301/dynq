package dynq.executor.read.fn

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.common.base.CharMatcher
import dynq.cli.command.ReadCommand
import dynq.executor.read.model.FilterOutput
import dynq.jq.jq
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.toList

private const val METADATA_PROP = "meta"
private const val CONTENT_PROP = "content"

suspend fun present(
    command: ReadCommand,
    outputChannel: Channel<FilterOutput>
) {
    if (command.stream()) {
        streamOutput(command, outputChannel)
        return
    }

    val limit = command.limit()
    val aggregator = command.aggregate()
    val contentOnly = command.contentOnly()
    val filterOutputs = outputChannel.toList()

    var expression = "flatten | .[0:${limit ?: ""}]"
    if (aggregator != null) {
        expression += " | $aggregator"
    }

    if (!contentOnly) {
        val metadata = aggregateMetadata(
            filterOutputs.map { it.meta },
            limit == null && command.concurrency() == 1
        )
        expression += " | {$METADATA_PROP: ${
            jacksonObjectMapper().writer().writeValueAsString(metadata)
        }, $CONTENT_PROP: .} | del(..|nulls)"
    }

    var content = CharMatcher.anyOf("\r\n\t").removeFrom(
        filterOutputs.map { it.items }
            .flatten()
            .toString()
    )
    if (command.rearrangeAttributes()) {
        content = jq(content, sortKeys = true)!!
    }

    jq(
        content,
        filter = expression,
        pretty = !command.compact(),
        colorize = colorize(command)
    ).let(::println)
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
                        colorize = colorize(command)
                    ).let(::println)
                }
            }.let { it?.size ?: 0 }

        if (remaining != null) {
            remaining -= found
            if (remaining <= 0) break
        }
    }
}

private fun colorize(command: ReadCommand): Boolean {
    return command.colorize() || System.console().isTerminal && !command.monochrome()
}
