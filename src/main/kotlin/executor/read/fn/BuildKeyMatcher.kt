package dynq.executor.read.fn

import dynq.executor.read.model.KeyMatcher
import dynq.jq.jqn
import dynq.jq.pipe
import dynq.jq.throwJqError
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.JsonItemAttributeConverter

fun buildPartitionKeyMatcher(filter: String?): KeyMatcher.Discrete? {
    return when (val key = buildKeyMatcher(filter)) {
        is KeyMatcher.Continuous -> throw Error("partition key values must be discrete")
        is KeyMatcher.Discrete -> key
        else -> null
    }
}

val buildSortKeyMatcher = ::buildKeyMatcher

private fun buildKeyMatcher(filter: String?): KeyMatcher? {
    if (filter == null) {
        return null
    }
    val onError = throwJqError("bad key filter")

    val name = jqn(
        input = "{}",
        filter = filter.pipe("keys[0]"),
        onError
    ).asString()
    val node = jqn(
        input = "{}",
        filter = filter.pipe("[.[]][0]"),
        onError
    )
    val converter = JsonItemAttributeConverter.create()

    if (node.isObject) {
        val map = converter.transformFrom(node).m()
        return KeyMatcher.Continuous(
            name,
            gt = map["greater_than"] ?: map["gt"],
            gte = map["greater_than_or_equals"] ?: map["gte"],
            lt = map["less_than"] ?: map["lt"],
            lte = map["less_than_or_equals"] ?: map["lte"],
            bw = map["begins_with"] ?: map["bw"]
        )
    }

    val nodes = if (node.isArray) {
        node.asArray()
    } else if (node.isString || node.isNumber) {
        listOf(node)
    } else {
        throw Error("key value must be string or number")
    }
    return KeyMatcher.Discrete(name, nodes.map { converter.transformFrom(it) })
}
