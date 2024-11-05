package dynq.executor.read.fn

import dynq.error.FriendlyError
import dynq.executor.read.model.KeyMatcher
import dynq.jq.jqn
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.JsonItemAttributeConverter

fun buildPartitionKeyMatcher(filter: String?): KeyMatcher.Values? {
    val key = buildKeyMatcher(filter)
    if (key is KeyMatcher.Range) {
        throw Error("partition key values must be discrete")
    }
    return key as KeyMatcher.Values? // TODO infer?
}

val buildSortKeyMatcher = ::buildKeyMatcher

private fun buildKeyMatcher(filter: String?): KeyMatcher? {
    if (filter == null) {
        return null
    }

    val name = jqn(
        input = "{}",
        filter = "$filter | keys[0]"
    )?.asString()
    val node = jqn(
        input = "{}",
        filter = "$filter | [.[]][0]"
    )
    if (name == null || node == null) {
        throw FriendlyError("bad key filter")
    }
    val converter = JsonItemAttributeConverter.create()

    if (node.isObject) {
        val map = converter.transformFrom(node).m()
        return KeyMatcher.Range(
            name,
            gt = map["gt"],
            gte = map["gte"],
            lt = map["lt"],
            lte = map["lte"],
            beg = map["beg"]
        )
    }

    val nodes = if (node.isArray) {
        node.asArray()
    } else if (node.isString || node.isNumber) {
        listOf(node)
    } else {
        throw Error("key value must be string or number")
    }
    return KeyMatcher.Values(name, nodes.map { converter.transformFrom(it) })
}
