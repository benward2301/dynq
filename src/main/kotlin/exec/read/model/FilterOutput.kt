package dynq.exec.read.model

import software.amazon.awssdk.protocols.jsoncore.JsonNode

data class FilterOutput(
    val items: List<JsonNode>,
    val meta: ReadMetadata
)