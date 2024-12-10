package dynq.executor.read

import dynq.cli.command.ReadCommand
import dynq.cli.route.CommandExecutor
import dynq.ddb.createDynamoDbClient
import dynq.executor.read.fn.*
import dynq.executor.read.model.FilterOutput
import dynq.executor.read.model.KeyMatcher
import dynq.executor.read.model.RawReadOutput
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

val executeRead = CommandExecutor<ReadCommand> { command ->
    val ddb = createDynamoDbClient(command.endpointUrl(), command.profile(), command.region())

    val readChannel = Channel<RawReadOutput>(command.concurrency())
    val outputChannel = Channel<FilterOutput>(Channel.UNLIMITED)

    val partitionKey = buildPartitionKeyMatcher(command.partitionKey())
    val sortKey = buildSortKeyMatcher(command.sortKey())

    runBlocking {
        val reading = launch(Dispatchers.IO) {
            when {
                partitionKey == null ->
                    scan(command, readChannel, ddb)

                sortKey is KeyMatcher.Discrete && command.globalIndexName() == null ->
                    getItems(command, readChannel, ddb, partitionKey, sortKey)

                else ->
                    query(command, readChannel, ddb, partitionKey, sortKey)
            }
            readChannel.close()
        }
        val filtering = launch {
            filter(ddb, command, readChannel, outputChannel)
        }
        launch {
            collate(command, outputChannel) {
                reading.cancelAndJoin()
                filtering.cancelAndJoin()
            }
        }
    }
}
