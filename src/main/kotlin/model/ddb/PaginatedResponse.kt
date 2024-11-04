package dynq.model.ddb

import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.ScanResponse

data class PaginatedResponse(
    val items: List<DynamoDbItem>,
    val consumedCapacity: Double,
    val scannedCount: Int,
    val lastEvaluatedKey: DynamoDbItem?
) {
    companion object {
        fun from(response: ScanResponse): PaginatedResponse {
            return PaginatedResponse(
                response.items(),
                response.consumedCapacity().capacityUnits(),
                response.scannedCount(),
                if (response.hasLastEvaluatedKey()) response.lastEvaluatedKey() else null
            )
        }

        fun from(response: QueryResponse): PaginatedResponse {
            return PaginatedResponse(
                response.items(),
                response.consumedCapacity().capacityUnits(),
                response.scannedCount(),
                if (response.hasLastEvaluatedKey()) response.lastEvaluatedKey() else null
            )
        }
    }

}

