package dynq.jq

import software.amazon.awssdk.protocols.jsoncore.JsonNode

fun jqn(
    input: String,
    filter: String
): JsonNode? {
    val str = jq(
        input = input,
        filter = filter
    )
    if (str == null) {
        return null
    }
    return JsonNode.parser().parse(str)
}