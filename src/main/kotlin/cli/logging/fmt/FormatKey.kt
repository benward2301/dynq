package cli.logging.fmt

import dynq.ddb.model.Key
import dynq.ddb.model.KeyMember
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

fun <S : KeyMember?> formatKey(key: Key<S>): String {
    return "(${unwrapValue(key.partition.value)}${(key.sort?.let { ") (${unwrapValue(it.value)}" } ?: "")})"
}

private fun unwrapValue(value: AttributeValue): String {
    return value.let { it.s() ?: it.n() }.toString()
}