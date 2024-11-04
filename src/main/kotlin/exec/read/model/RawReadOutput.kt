package dynq.exec.read.model

import dynq.model.ddb.DynamoDbItem

data class RawReadOutput(
    val items: List<DynamoDbItem>,
    val meta: ReadMetadata,
    var batchSize: Int? = null
)