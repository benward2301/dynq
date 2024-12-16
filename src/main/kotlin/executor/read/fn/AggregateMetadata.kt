package dynq.executor.read.fn

import dynq.executor.read.model.ReadMetadata

fun aggregateMetadata(
    entries: List<ReadMetadata>,
    includeLastEvaluatedKey: Boolean
): ReadMetadata {
    var requestCount = 0
    var consumedCapacity = 0.0
    var scannedCount: Int? = null
    var hitCount: Int? = null

    for (metadata in entries) {
        requestCount += metadata.requestCount
        consumedCapacity += metadata.consumedCapacity
        if (metadata.scannedCount != null) {
            scannedCount = (scannedCount ?: 0) + metadata.scannedCount
        }
        if (metadata.hitCount != null) {
            hitCount = (hitCount ?: 0) + metadata.hitCount
        }
    }

    val last = entries.last()

    return ReadMetadata(
        consumedCapacity,
        requestCount = requestCount,
        scannedCount = scannedCount,
        hitCount = hitCount,
        lastEvaluatedKey = if (includeLastEvaluatedKey) last.lastEvaluatedKey else null
    )
}