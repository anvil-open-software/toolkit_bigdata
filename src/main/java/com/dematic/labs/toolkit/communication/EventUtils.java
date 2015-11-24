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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.LongStream;

/**
 * Utility class to support events.
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

    /**
     * Generate analytic system events.
     *
     * @param numberOfEvents -- # of events to generate
     * @param nodeId         -- amount of nodes
     * @return List<Event>   -- list of generated events
     */
    public static List<Event> generateEvents(final long numberOfEvents, final String nodeId) {
        // startInclusive the (inclusive) initial value, endExclusive the exclusive upper bound
        return LongStream.range(1, numberOfEvents + 1)
                .parallel()
                .mapToObj(value -> new Event(UUID.randomUUID(), EventSequenceNumber.next(), nodeId, null,
                        DateTime.now(), null, null))
                        //supplier, accumulator, combiner
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    public static DateTime now() {
        return DateTime.now().toDateTimeISO();
    }

    public static String nowString() {
        return now().toString();
    }

    public static DateTime dateTime(final long inMillis) {
        return new DateTime(inMillis).toDateTimeISO();
    }

    private final static class EventSerializer extends JsonSerializer<Event> {
        @Override
        public void serialize(final Event event, final JsonGenerator jsonGenerator, final SerializerProvider provider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("id", event.getId().toString());
            jsonGenerator.writeNumberField("sequence", event.getSequence());
            jsonGenerator.writeStringField("nodeId", event.getNodeId());
            jsonGenerator.writeStringField("type", event.getType());
            jsonGenerator.writeStringField("timestamp", event.getTimestamp().toString());
            jsonGenerator.writeStringField("generatorId", event.getGeneratorId());
            final Long version = event.getVersion();
            if (version != null) {
                jsonGenerator.writeNumberField("version", version);
            }
            jsonGenerator.writeEndObject();
        }
    }

    private final static class EventDeserializer extends JsonDeserializer<Event> {
        @Override
        public Event deserialize(final JsonParser jp, final DeserializationContext deserializationContext) throws IOException {
            final ObjectCodec codec = jp.getCodec();
            final JsonNode jsonNode = codec.readTree(jp);
            final JsonNode eventIdNode = jsonNode.get("id");
            if (eventIdNode == null || isNullOrEmpty(eventIdNode.asText())) {
                throw new IllegalStateException("Event does not have an id");
            }
            final UUID uuid = UUID.fromString(eventIdNode.asText());

            final JsonNode sequenceNode = jsonNode.get("sequence");
            if (sequenceNode == null || sequenceNode.asLong() == 0) {
                throw new IllegalStateException("Event does not have a sequence number assigned");
            }
            final long sequence = sequenceNode.asLong();

            final JsonNode nodeIdNode = jsonNode.get("nodeId");
            if (nodeIdNode == null || isNullOrEmpty(nodeIdNode.asText())) {
                throw new IllegalStateException("Event does not have a nodeId assigned");
            }
            final String nodeId = nodeIdNode.asText();

            final JsonNode typeNode = jsonNode.get("type");
            final String type = typeNode == null ? null : typeNode.asText();

            final JsonNode timestampNode = jsonNode.get("timestamp");
            if (timestampNode == null || isNullOrEmpty(timestampNode.asText())) {
                throw new IllegalStateException("Event does not have an generated timestamp");
            }
            final DateTime timestamp = new DateTime(timestampNode.asText());

            final JsonNode generatorIdNode = jsonNode.get("generatorId");
            final String generatorId = generatorIdNode == null ? null : generatorIdNode.asText();

            final JsonNode versionNode = jsonNode.get("version");
            final Long version = versionNode == null ? null : versionNode.asLong();

            return new Event(uuid, sequence, nodeId, type, timestamp, generatorId, version);
        }

        // use a lib
        private static boolean isNullOrEmpty(final String string) {
            return string == null || string.length() == 0;
        }
    }
}