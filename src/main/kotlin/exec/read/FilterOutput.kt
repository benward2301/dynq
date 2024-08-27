package dynq.exec.read

import software.amazon.awssdk.protocols.jsoncore.JsonNode

data class FilterOutput(
    val items: List<JsonNode>,
    val meta: ReadMetadata
)