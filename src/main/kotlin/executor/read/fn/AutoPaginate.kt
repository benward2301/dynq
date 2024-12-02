package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.cli.whisper
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
    coroutineNumber: Int = 0,
    read: (
        startKey: DynamoDbItem?,
        remaining: Int?
    ) -> PaginatedResponse
) {
    var scannedCount = 0
    val scanLimit = command.scanLimit()
    var startKey = command.startKey()?.let {
        EnhancedDocument.fromJson(
            jq(input = it)
        ).toMap()
    }
    do {
        if (startKey != null) {
            whisper(coroutineNumber) { "Scanning from $startKey" }
        }
        val response = read(startKey, scanLimit)
        channel.send(
            RawReadOutput(
                response.items,
                ReadMetadata(
                    requestType,
                    response.consumedCapacity,
                    scannedCount = response.scannedCount,
                    lastEvaluatedKey = response.lastEvaluatedKey
                )
            )
        )
        scannedCount += response.scannedCount
        whisper(coroutineNumber) { "${response.scannedCount} item(s) scanned" }

        startKey = response.lastEvaluatedKey
    } while (startKey != null && (scanLimit == null || scannedCount < scanLimit))
}
