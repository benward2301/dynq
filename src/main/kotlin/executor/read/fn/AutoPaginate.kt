package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.ddb.model.DynamoDbItem
import dynq.ddb.model.PaginatedResponse
import dynq.executor.read.model.RawReadOutput
import dynq.executor.read.model.ReadMetadata
import dynq.jq.jq
import kotlinx.coroutines.channels.Channel
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument

suspend fun autoPaginate(
    command: ReadCommand,
    channel: Channel<RawReadOutput>,
    requestType: String,
    read: (
        startKey: DynamoDbItem?,
        remaining: Int?
    ) -> PaginatedResponse
) {
    var remaining = command.scanLimit()
    var startKey = command.startKey()?.let {
        EnhancedDocument.fromJson(
            jq(input = it)
        ).toMap()
    }
    do {
        val response = read(startKey, remaining)
        channel.send(
            RawReadOutput(
                response.items,
                ReadMetadata(
                    requestType,
                    response.consumedCapacity,
                    scannedCount = response.scannedCount,
                    lastEvaluatedKey = response.lastEvaluatedKey
                ),
                command.concurrency()
            )
        )
        if (remaining != null) {
            remaining -= response.scannedCount
        }
        startKey = response.lastEvaluatedKey
    } while (startKey != null && (remaining ?: 1) > 0)
}