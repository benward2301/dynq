package dynq.executor.read.fn

import dynq.ddb.model.ReadProjection

fun sanitizeProjectionExpression(
    expression: String?,
    exprAttrNames: Map<String, String>? = null
): ReadProjection {
    if (expression == null) {
        return ReadProjection(null, exprAttrNames)
    }
    val names = expression.split(",").map(String::trim)
    val aliases = mutableListOf<String>()
    val map = mutableMapOf<String, String>()
    if (exprAttrNames != null) {
        map.putAll(exprAttrNames)
    }
    for (i in names.indices) {
        val alias = "#a$i"
        map[alias] = names[i]
        aliases.add(alias)
    }
    return ReadProjection(aliases.joinToString(", "), map)
}