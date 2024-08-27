package dynq.model

data class PaginatedResponse(
    val items: List<DynamoDbItem>,
    val consumedCapacity: Double,
    val scannedCount: Int,
    val lastEvaluatedKey: DynamoDbItem?
)
