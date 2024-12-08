package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.cli.whisper
import dynq.ddb.model.PaginatedResponse
import dynq.executor.read.model.RawReadOutput
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import java.util.concurrent.atomic.AtomicInteger

suspend fun scan(
    command: ReadCommand,
    readChannel: Channel<RawReadOutput>,
    ddb: DynamoDbClient,
) = coroutineScope {
    whisper { "Scanning table: ${command.tableName()}" }
    val scannedCount = AtomicInteger()

    for (segment in 0..<command.concurrency()) {
        launch {
            autoPaginate(
                command,
                readChannel,
                "Scan",
                segment
            ) { startKey, limit ->
                val request = buildScanBase(command)
                    .segment(segment)
                    .exclusiveStartKey(startKey)
                    .limit(limit)
                    .build()
                val response = ddb.scan(request)
                scannedCount.addAndGet(response.scannedCount())
                PaginatedResponse.from(response)
            }
            whisper(segment) { "Segment scan complete" }
        }
    }
}

private fun buildScanBase(command: ReadCommand): ScanRequest.Builder {
    return ScanRequest.builder()
        .tableName(command.tableName())
        .totalSegments(command.concurrency())
        .consistentRead(command.consistentRead())
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .apply { sanitizeProjectionExpression(command.projectionExpression()).applyTo(this) }
}