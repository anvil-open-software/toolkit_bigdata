package com.dematic.labs.toolkit.communication;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

/**
 * Utility class to support events.
 *
 */
public final class EventUtils {
    private final static ObjectMapper objectMapper;

    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(Event.class, new EventSerializer());
        module.addDeserializer(Event.class, new EventDeserializer());
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(module);
    }

    private EventUtils() {
    }

    public static Event jsonToEvent(final String json) throws IOException {
        return objectMapper.readValue(json, Event.class);
    }

    public static String eventToJson(final Event event) throws IOException {
        return objectMapper.writeValueAsString(event);
    }

    public static byte[] eventToJsonByteArray(final Event event) throws IOException {
        return eventToJson(event).getBytes(Charset.defaultCharset());
    }

    private final static class EventSerializer extends JsonSerializer<Event> {
        @Override
        public void serialize(final Event event, final JsonGenerator jsonGenerator, final SerializerProvider provider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("eventId", event.getEventId().toString());
            jsonGenerator.writeNumberField("nodeId", event.getNodeId());
            jsonGenerator.writeNumberField("orderId", event.getOrderId());
            jsonGenerator.writeStringField("timestamp", event.getTimestamp().toString());
            jsonGenerator.writeNumberField("value", event.getValue());
            jsonGenerator.writeEndObject();
        }
    }

    private final static class EventDeserializer extends JsonDeserializer<Event> {
        @Override
        public Event deserialize(final JsonParser jp, final DeserializationContext deserializationContext) throws IOException {
            final ObjectCodec codec = jp.getCodec();
            final JsonNode jsonNode = codec.readTree(jp);
            final JsonNode eventIdNode = jsonNode.get("eventId");
            if (eventIdNode == null || isNullOrEmpty(eventIdNode.asText()) ) {
                throw new IllegalStateException("Event does not have an eventId");
            }
            final UUID uuid = UUID.fromString(eventIdNode.asText());

            final JsonNode nodeIdNode = jsonNode.get("nodeId");
            if(nodeIdNode == null || nodeIdNode.asInt() == 0) {
                throw new IllegalStateException("Event does not have a nodeId assigned");
            }
            final int nodeId = nodeIdNode.asInt();

            final JsonNode orderIdNode = jsonNode.get("orderId");
            if(orderIdNode == null || orderIdNode.asInt() == 0) {
                throw new IllegalStateException("Event does not have a orderId assigned");
            }
            final int orderId = orderIdNode.asInt();

            final JsonNode timestampNode = jsonNode.get("timestamp");
            if (timestampNode == null || isNullOrEmpty(timestampNode.asText())) {
                throw new IllegalStateException("Event does not have an generated timestamp");
            }
            final DateTime timestamp = new DateTime(timestampNode.asText());

            final JsonNode valueNode = jsonNode.get("value");
            if(valueNode == null || valueNode.asDouble() == 0) {
                throw new IllegalStateException("Event does not have a value assigned");
            }
            final double value = valueNode.asDouble();

            return new Event(uuid, nodeId, orderId, timestamp, value);
        }

        // use a lib
        private static boolean isNullOrEmpty(final String string) {
            return string == null || string.length() == 0;
        }
    }
}