package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.cli.logging.*
import cli.logging.fmt.formatProgressMessage
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
    le: LogEntry,
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
    logScanProgress(le, scannedCount)

    do {
        val response = read(
            startKey,
            calculateNextScanLimit(scannedCount, scanLimit, itemsPerRequest)
        )
        scannedCount += response.scannedCount

        channel.send(buildReadOutput(response))
        logScanProgress(le, scannedCount)

        startKey = response.lastEvaluatedKey
        requestCount++
    } while (
        startKey != null &&
        isCountWithinLimit(scannedCount, scanLimit) &&
        isCountWithinLimit(requestCount, requestLimit)
    )
    logScanResult(le, scannedCount)
}

private fun buildReadOutput(
    response: PaginatedResponse
): RawReadOutput {
    return RawReadOutput(
        response.items,
        ReadMetadata(
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

private fun logScanProgress(le: LogEntry, scannedCount: Int) {
    le.log { formatProgressMessage(style(RED)("$SPINNER"), le.label, describeScannedCount(scannedCount)) }
}

private fun logScanResult(le: LogEntry, scannedCount: Int) {
    le.log {
        val icon: String
        val message: String
        if (scannedCount == 0) {
            icon = style(BOLD, YELLOW)("-")
            message = "Empty segment"
        } else {
            icon = style(GREEN)("$CHECK_MARK")
            message = describeScannedCount(scannedCount)
        }
        formatProgressMessage(icon, le.label, message)
    }
}

private fun describeScannedCount(scannedCount: Int) = "$scannedCount item(s) scanned"