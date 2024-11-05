package dynq.executor.read.model

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

sealed class KeyMatcher {

    abstract val name: String

    data class Values(
        override val name: String,
        val values: List<AttributeValue>
    ) : KeyMatcher()

    data class Range(
        override val name: String,
        val gt: AttributeValue?,
        val gte: AttributeValue?,
        val lt: AttributeValue?,
        val lte: AttributeValue?,
        val beg: AttributeValue?
    ) : KeyMatcher() {

        fun between(): Pair<AttributeValue, AttributeValue>? {
            if (gte == null || lte == null) {
                return null
            }
            return Pair(gte, lte)
        }

    }

}