package dynq.executor.read.fn

import dynq.logging.fmt.formatProgressMessage
import dynq.cli.command.ReadCommand
import dynq.cli.route.CommandBinding
import dynq.ddb.model.DynamoDbItem
import dynq.ddb.model.PaginatedResponse
import dynq.executor.read.model.RawReadOutput
import dynq.executor.read.model.ReadMetadata
import dynq.jq.jq
import dynq.jq.pipe
import dynq.jq.pipeToNonNull
import dynq.jq.throwJqError
import dynq.logging.*
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
    var startKey = buildInitialStartKey(command)
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

private fun buildInitialStartKey(command: ReadCommand): DynamoDbItem? {
    return command.startKey()?.let {
        EnhancedDocument.fromJson(
            jq(
                input = "{}",
                filter = (command.partitionKey()?.pipe("map_values([.] | flatten | .[0])"))
                    .pipeToNonNull(". + ($it)"),
                onError = throwJqError("bad start key filter")
            )
        ).toMap()
    }
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
    if (CommandBinding.global.logMode() == LogMode.RENDER) {
        le.log { formatProgressMessage(style(RED)("$SPINNER"), le.label, describeScannedCount(scannedCount)) }
    }
}

private fun logScanResult(le: LogEntry, scannedCount: Int) {
    le.log {
        val icon: String
        val message: String
        if (scannedCount == 0) {
            icon = style(YELLOW)("$EM_DASH")
            message = "Empty segment"
        } else {
            icon = style(GREEN)("$CHECK_MARK")
            message = describeScannedCount(scannedCount)
        }
        formatProgressMessage(icon, le.label, message)
    }
}

private fun describeScannedCount(scannedCount: Int) = "$scannedCount item(s) scanned"