package dynq.exec.read

import com.fasterxml.jackson.databind.annotation.JsonSerialize
import dynq.model.DynamoDbItem
import dynq.model.DynamoDbItemSerializer

data class ReadMetadata(
    val requestType: String,
    val consumedCapacity: Double,
    val requestCount: Int = 1,
    val scannedCount: Int? = null,
    val hitCount: Int? = null,
    @JsonSerialize(using = DynamoDbItemSerializer::class)
    val lastEvaluatedKey: DynamoDbItem? = null
)