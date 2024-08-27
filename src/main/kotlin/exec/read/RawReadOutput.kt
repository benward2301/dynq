package dynq.exec.read

import dynq.model.DynamoDbItem

data class RawReadOutput(
    val items: List<DynamoDbItem>,
    val meta: ReadMetadata,
    var batchSize: Int? = null
)