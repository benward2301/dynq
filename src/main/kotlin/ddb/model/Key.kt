package dynq.ddb.model

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

typealias QueryKey = Key<KeyMember?>
typealias GetItemKey = Key<KeyMember>

data class Key<S : KeyMember?>(
    val partition: KeyMember,
    val sort: S
)

data class KeyMember(
    val name: String,
    val value: AttributeValue
)
