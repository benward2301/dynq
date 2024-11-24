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

    val filterOutputs = outputChannel.toList()

    CharMatcher.anyOf("\r\n\t").removeFrom(
        filterOutputs.map { it.items }
            .flatten()
            .toString()
    ).let {
        jq(
            it,
            filter = buildAggregationFilter(command),
            sortKeys = command.rearrangeAttributes()
        )
    }.let {
        if (it == null) {
            throw Error("bad aggregation filter")
        }
        jq(
            it,
            filter = buildPresentationFilter(command, filterOutputs),
            pretty = !command.compact(),
            colorize = colorize(command)
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

private fun buildAggregationFilter(command: ReadCommand): String {
    fun String.pipe(arg: String?): String {
        return if (arg == null) this else "$this | $arg"
    }

    val limit = command.limit()?.let { ".[0:$it]" }

    return if (command.reduce() == null) {
        "flatten".pipe(limit)
            .pipe(command.aggregate())
    } else {
        ".[0]".pipe(limit)
    }
}

private fun buildPresentationFilter(
    command: ReadCommand,
    filterOutputs: List<FilterOutput>
): String? {
    return if (command.contentOnly()) null else
        "{$METADATA_PROP: ${
            jacksonObjectMapper().writer().writeValueAsString(
                aggregateMetadata(
                    filterOutputs.map { it.meta },
                    command.limit() == null && command.concurrency() == 1
                )
            )
        }, $CONTENT_PROP: .} | del(..|nulls)"
}

private fun colorize(command: ReadCommand): Boolean {
    return command.colorize() || System.console()?.isTerminal == true && !command.monochrome()
}
