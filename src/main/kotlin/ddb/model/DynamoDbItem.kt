package dynq.ddb.model

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.util.Base64

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

class RawDynamoDbItemSerializer : JsonSerializer<DynamoDbItem?>() {

    override fun serialize(
        item: DynamoDbItem?,
        generator: JsonGenerator,
        provider: SerializerProvider
    ) {
        if (item == null) {
            generator.writeNull()
            return
        }
        generator.writeStartObject()
        for ((key, value) in item) {
            generator.writeFieldName(key)
            serializeAttributeValue(generator, value)
        }
        generator.writeEndObject()
    }

    private fun serializeAttributeValue(gen: JsonGenerator, av: AttributeValue) {
        gen.writeStartObject()
        when (av.type()) {
            AttributeValue.Type.S -> gen.writeStringField("S", av.s())
            AttributeValue.Type.N -> gen.writeStringField("N", av.n())
            AttributeValue.Type.B -> gen.writeStringField("B", Base64.getEncoder().encodeToString(av.b().asByteArray()))
            AttributeValue.Type.SS -> { gen.writeFieldName("SS"); gen.writeObject(av.ss()) }
            AttributeValue.Type.NS -> { gen.writeFieldName("NS"); gen.writeObject(av.ns()) }
            AttributeValue.Type.BS -> { gen.writeFieldName("BS"); gen.writeObject(av.bs().map { Base64.getEncoder().encodeToString(it.asByteArray()) }) }
            AttributeValue.Type.BOOL -> gen.writeBooleanField("BOOL", av.bool())
            AttributeValue.Type.NUL -> gen.writeBooleanField("NULL", true)
            AttributeValue.Type.L -> {
                gen.writeFieldName("L")
                gen.writeStartArray()
                av.l().forEach { serializeAttributeValue(gen, it) }
                gen.writeEndArray()
            }
            AttributeValue.Type.M -> {
                gen.writeFieldName("M")
                gen.writeStartObject()
                av.m().forEach { (k, v) ->
                    gen.writeFieldName(k)
                    serializeAttributeValue(gen, v)
                }
                gen.writeEndObject()
            }
            else -> gen.writeNullField("NULL")
        }
        gen.writeEndObject()
    }

}