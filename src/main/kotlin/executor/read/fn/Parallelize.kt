package dynq.executor.read.fn

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

suspend fun <T> parallelize(
    concurrency: Int,
    inputs: Collection<T>,
    consume: suspend (item: T) -> Unit
) = coroutineScope {
    val channel = Channel<T>(Channel.UNLIMITED)
    inputs.forEach { channel.send(it) }
    channel.close()

    parallelize(concurrency.coerceAtMost(inputs.size)) {
        for (input in channel) {
            consume(input)
        }
    }
}

suspend fun parallelize(
    concurrency: Int,
    run: suspend (n: Int) -> Unit
) = coroutineScope {
    for (n in 0..<concurrency) {
        launch {
            run(n)
        }
    }
}
