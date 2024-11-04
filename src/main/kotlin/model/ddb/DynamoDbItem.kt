package dynq.model.ddb

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

typealias DynamoDbItem = Map<String, AttributeValue>

class DynamoDbItemSerializer : JsonSerializer<DynamoDbItem?>() {

    override fun serialize(
        item: DynamoDbItem?,
        generator: JsonGenerator,
        provider: SerializerProvider
    ) {
        if (item != null) {
            generator.writeObject(
                ObjectMapper().readerFor(Map::class.java).readValue(
                    EnhancedDocument.fromAttributeValueMap(item).toJson()
                )
            )
        }
    }

}