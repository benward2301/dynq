package dynq.executor.read.fn

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.common.base.CharMatcher
import dynq.cli.command.ReadCommand
import dynq.error.FriendlyError
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
    val limit = command.limit()
    val aggregator = command.aggregate()
    val contentOnly = command.contentOnly()
    val resultSets = outputChannel.toList()

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