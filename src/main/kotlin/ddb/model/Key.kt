package dynq.ddb.model

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

data class Key(
    val partition: KeyMember,
    val sort: KeyMember?
)

data class KeyMember(
    val name: String,
    val value: AttributeValue
)
