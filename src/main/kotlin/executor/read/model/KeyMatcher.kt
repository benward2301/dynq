package dynq.executor.read.model

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

sealed class KeyMatcher {

    abstract val name: String

    data class Discrete(
        override val name: String,
        val values: List<AttributeValue>
    ) : KeyMatcher()

    data class Continuous(
        override val name: String,
        val gt: AttributeValue?,
        val gte: AttributeValue?,
        val lt: AttributeValue?,
        val lte: AttributeValue?,
        val bw: AttributeValue?
    ) : KeyMatcher() {

        fun between(): Pair<AttributeValue, AttributeValue>? {
            if (gte == null || lte == null) {
                return null
            }
            return Pair(gte, lte)
        }

    }

}