package dynq.executor.read.fn

import dynq.executor.read.model.KeyMatcher
import dynq.jq.jqn
import dynq.jq.pipe
import dynq.jq.throwJqError
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.JsonItemAttributeConverter

val buildPartitionKeyMatcher = fun(filter: String?): KeyMatcher.Discrete? {
    val descriptor = "partition"
    return buildKeyMatcher(descriptor)(filter).let {
        if (it is KeyMatcher.Discrete?) it
        else rejectKeyFilter(descriptor)
    }
}

val buildSortKeyMatcher = buildKeyMatcher("sort")

private fun buildKeyMatcher(descriptor: String) = fun(filter: String?): KeyMatcher? {
    if (filter == null) {
        return null
    }
    val onError = throwJqError("bad $descriptor key filter")

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
            beginsWith = map["begins_with"],
            between = map["between"]?.l()
                ?.let { Pair(it[0], it[1]) }
        )
    }

    val nodes = when {
        node.isArray ->
            node.asArray().takeUnless { list ->
                list.all { !it.isNumber && !it.isString }
            }

        node.isString || node.isNumber ->
            listOf(node)

        else -> null
    }
    if (nodes == null) {
        rejectKeyFilter(descriptor)
    }
    return KeyMatcher.Discrete(name, nodes.map { converter.transformFrom(it) })
}

private fun rejectKeyFilter(descriptor: String): Nothing {
    throw IllegalArgumentException("$descriptor key filter produced an illegal value")
}