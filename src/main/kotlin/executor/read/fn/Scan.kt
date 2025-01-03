package dynq.executor.read.fn

import dynq.cli.command.ReadCommand
import dynq.cli.logging.*
import cli.logging.fmt.formatRequestOp
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
    LogEntry.new(pos = 0).log { "${formatRequestOp("SCAN")} ${command.tableName()}" }

    val scannedCount = AtomicInteger()

    parallelize(command.concurrency()) { segment ->
        launch {
            autoPaginate(
                command,
                readChannel,
                LogEntry.new(indent = 1, pos = 1)
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