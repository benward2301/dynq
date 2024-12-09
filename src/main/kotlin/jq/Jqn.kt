package dynq.jq

import software.amazon.awssdk.protocols.jsoncore.JsonNode

fun jqn(
    input: String,
    filter: String,
    label: String? = null
): JsonNode {
    return JsonNode.parser().parse(
        jq(
            input = input,
            filter = filter,
            label = label
        )
    )
}