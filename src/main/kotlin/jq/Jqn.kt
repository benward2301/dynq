package dynq.jq

import com.arakelian.jq.JqResponse
import software.amazon.awssdk.protocols.jsoncore.JsonNode

fun jqn(
    input: String,
    filter: String,
    onError: (JqResponse) -> Unit
): JsonNode {
    return JsonNode.parser().parse(
        jq(
            input = input,
            filter = filter,
            onError = onError
        )
    )
}