package dynq.model.ddb

import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

data class ReadProjection(
    val projectionExpression: String?,
    val expressionAttributeNames: Map<String, String>?
) {
    fun applyTo(builder: ScanRequest.Builder) {
        builder.projectionExpression(this.projectionExpression)
            .expressionAttributeNames(this.expressionAttributeNames)
    }

    fun applyTo(builder: QueryRequest.Builder) {
        builder.projectionExpression(this.projectionExpression)
            .expressionAttributeNames(this.expressionAttributeNames)
    }

    fun applyTo(builder: GetItemRequest.Builder) {
        builder.projectionExpression(this.projectionExpression)
            .expressionAttributeNames(this.expressionAttributeNames)
    }

    fun applyTo(builder: KeysAndAttributes.Builder) {
        builder.projectionExpression(this.projectionExpression)
            .expressionAttributeNames(this.expressionAttributeNames)
    }
}
