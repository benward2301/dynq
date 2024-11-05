package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

suspend fun <T> parallelize(
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