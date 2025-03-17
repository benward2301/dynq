package dynq.logging.fmt

import dynq.ddb.model.Key
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

fun formatKey(key: Key): String {
    return "(${unwrapValue(key.partition.value)}${(key.sort?.let { ") (${unwrapValue(it.value)}" } ?: "")})"
}

private fun unwrapValue(value: AttributeValue): String {
    return value.let { it.s() ?: it.n() }.toString()
}