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
import java.util.Random;
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
     * @param nodeSize       -- amount of nodes
     * @param orderSize      -- amout of orders
     * @return List<Event>   -- list of generated events
     */
    public static List<Event> generateEvents(final long numberOfEvents, final int nodeSize, final int orderSize) {
        final Random randomGenerator = new Random();
        // startInclusive the (inclusive) initial value, endExclusive the exclusive upper bound
        return LongStream.range(1, numberOfEvents + 1)
                .parallel()
                .mapToObj(value -> new Event(UUID.randomUUID(), EventSequenceNumber.next(),
                        generateNode(nodeSize, randomGenerator),
                        generateOrder(orderSize, randomGenerator), DateTime.now(),
                        generateValue(nodeSize, orderSize, randomGenerator)))
                        //supplier, accumulator, combiner
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    public static int generateNode(final int nodeSize, final Random randomGenerator) {
        return randomGenerator.nextInt(nodeSize) + 1;
    }

    public static int generateOrder(final int orderSize, final Random randomGenerator) {
        return randomGenerator.nextInt(orderSize) + 1;
    }

    public static double generateValue(final int nodeSize, final int orderSize, final Random randomGenerator) {
        return Math.abs((int) Math.round(randomGenerator.nextGaussian() * orderSize + nodeSize));
    }

    private final static class EventSerializer extends JsonSerializer<Event> {
        @Override
        public void serialize(final Event event, final JsonGenerator jsonGenerator, final SerializerProvider provider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("eventId", event.getEventId().toString());
            jsonGenerator.writeNumberField("sequence", event.getSequence());
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
            if (eventIdNode == null || isNullOrEmpty(eventIdNode.asText())) {
                throw new IllegalStateException("Event does not have an eventId");
            }
            final UUID uuid = UUID.fromString(eventIdNode.asText());

            final JsonNode sequenceNode = jsonNode.get("sequence");
            if (sequenceNode == null || sequenceNode.asLong() == 0) {
                throw new IllegalStateException("Event does not have a sequence number assigned");
            }
            final long sequence = sequenceNode.asLong();

            final JsonNode nodeIdNode = jsonNode.get("nodeId");
            if (nodeIdNode == null || nodeIdNode.asInt() == 0) {
                throw new IllegalStateException("Event does not have a nodeId assigned");
            }
            final int nodeId = nodeIdNode.asInt();

            final JsonNode orderIdNode = jsonNode.get("orderId");
            if (orderIdNode == null || orderIdNode.asInt() == 0) {
                throw new IllegalStateException("Event does not have a orderId assigned");
            }
            final int orderId = orderIdNode.asInt();

            final JsonNode timestampNode = jsonNode.get("timestamp");
            if (timestampNode == null || isNullOrEmpty(timestampNode.asText())) {
                throw new IllegalStateException("Event does not have an generated timestamp");
            }
            final DateTime timestamp = new DateTime(timestampNode.asText());

            final JsonNode valueNode = jsonNode.get("value");
            // todo: for now we are using random dist of numbers, so some may be negitive, put a positive number check,
            // todo: when this is changed
            if (valueNode == null) {
                throw new IllegalStateException("Event does not have a value assigned");
            }
            final double value = valueNode.asDouble();

            return new Event(uuid, sequence, nodeId, orderId, timestamp, value);
        }

        // use a lib
        private static boolean isNullOrEmpty(final String string) {
            return string == null || string.length() == 0;
        }
    }
}