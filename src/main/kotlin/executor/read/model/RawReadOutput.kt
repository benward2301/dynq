package dynq.executor.read.model

import dynq.ddb.model.DynamoDbItem

data class RawReadOutput(
    val items: List<DynamoDbItem>,
    val meta: ReadMetadata
)