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
import kotlin.math.min

suspend fun autoPaginate(
    command: ReadCommand,
    channel: Channel<RawReadOutput>,
    requestType: String,
    coroutineNumber: Int = 0,
    read: (
        startKey: DynamoDbItem?,
        limit: Int?
    ) -> PaginatedResponse
) {
    val scanLimit = command.scanLimit()
    val requestLimit = command.requestLimit()
    val itemsPerRequest = command.itemsPerRequest()

    var scannedCount = 0
    var requestCount = 0
    var startKey = command.startKey()?.let {
        EnhancedDocument.fromJson(
            jq(input = it)
        ).toMap()
    }
    do {
        if (startKey != null) {
            whisper(coroutineNumber) { "Scanning from $startKey" }
        }
        val response = read(startKey, calculateNextScanLimit(scannedCount, scanLimit, itemsPerRequest))
        channel.send(
            buildReadOutput(requestType, response)
        )
        scannedCount += response.scannedCount
        whisper(coroutineNumber) { "${response.scannedCount} item(s) scanned" }

        startKey = response.lastEvaluatedKey
        requestCount++
    } while (
        startKey != null &&
        isCountWithinLimit(scannedCount, scanLimit) &&
        isCountWithinLimit(requestCount, requestLimit)
            .also { if (!it) whisper(coroutineNumber) { "Request limit reached" } }
    )
}

private fun buildReadOutput(
    requestType: String,
    response: PaginatedResponse
): RawReadOutput {
    return RawReadOutput(
        response.items,
        ReadMetadata(
            requestType,
            response.consumedCapacity,
            scannedCount = response.scannedCount,
            lastEvaluatedKey = response.lastEvaluatedKey
        )
    )
}

private fun calculateNextScanLimit(scannedCount: Int, scanLimit: Int?, itemsPerRequest: Int?): Int? {
    if (scanLimit == null) {
        return itemsPerRequest
    }
    return (scanLimit - scannedCount).also {
        if (itemsPerRequest != null) min(it, itemsPerRequest)
    }
}

private fun isCountWithinLimit(count: Int, limit: Int?): Boolean {
    return limit == null || count < limit
}