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
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class SignalUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SignalUtils.class);
    private final static ObjectMapper objectMapper;

    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(Signal.class, new SignalSerializer());
        module.addDeserializer(Signal.class, new SignalDeserializer());
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(module);
    }

    private SignalUtils() {
    }

    public static Signal jsonToSignal(final String json) throws IOException {
        return objectMapper.readValue(json, Signal.class);
    }

    public static String signalToJson(final Signal signal) throws IOException {
        return objectMapper.writeValueAsString(signal);
    }

    private final static class SignalSerializer extends JsonSerializer<Signal> {
        @Override
        public void serialize(final Signal signal, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializers)
                throws IOException {
            try {
                jsonGenerator.writeStartArray();
                jsonGenerator.writeStartObject();
                // write the ExtendedProperties array
                jsonGenerator.writeFieldName("ExtendedProperties");
                jsonGenerator.writeStartArray();
                signal.getExtendedProperties().forEach(property -> {
                    try {
                        jsonGenerator.writeString(property);
                    } catch (final IOException ioe) {
                        LOGGER.error("Unexpected error writing property {}", property, ioe);
                    }
                });
                jsonGenerator.writeEndArray();
                jsonGenerator.writeStringField("ProxiedTypeName", signal.getProxiedTypeName());
                jsonGenerator.writeNumberField("OPCTagID", Long.valueOf(signal.getOpcTagId()));
                jsonGenerator.writeNumberField("OPCTagReadingID", toLong(signal.getOpcTagReadingId()));
                jsonGenerator.writeNumberField("Quality", toLong(signal.getQuality()));
                jsonGenerator.writeStringField("Timestamp", signal.getTimestamp());
                jsonGenerator.writeStringField("Value", signal.getValue());
                jsonGenerator.writeNumberField("ID", toLong(signal.getId()));
                if (Strings.isNullOrEmpty(signal.getUniqueId())) {
                    jsonGenerator.writeNullField("UniqueID");
                } else {
                    jsonGenerator.writeStringField("UniqueID", signal.getUniqueId());
                }
            } finally {
                jsonGenerator.writeEndObject();
                jsonGenerator.writeEndArray();
                jsonGenerator.close();
            }
        }
    }

    /**
     * Convert to long.
     *
     * @param value -- value to convert
     * @return 0 or long value
     */
    private static long toLong(final String value) {
        return Strings.isNullOrEmpty(value) ? 0L : Long.valueOf(value);
    }

    private final static class SignalDeserializer extends JsonDeserializer<Signal> {
        @Override
        public Signal deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException {
            final ObjectCodec codec = jp.getCodec();
            final JsonNode jsonNode = codec.readTree(jp);

            final JsonNode extendedPropertiesNode = jsonNode.findValue("ExtendedProperties");
            final List<String> extendedProperties = new ArrayList<>();
            if (extendedPropertiesNode != null) {
                for (final JsonNode next : extendedPropertiesNode) {
                    final String property = next.isNull() ? null : next.asText();
                    extendedProperties.add(property);
                }
            }

            final JsonNode proxiedTypeNameNode = jsonNode.findValue("ProxiedTypeName");
            final String proxiedTypeName = proxiedTypeNameNode == null ? null : proxiedTypeNameNode.asText();

            final JsonNode opcTagIDNode = jsonNode.findValue("OPCTagID");
            final String opcTagID = opcTagIDNode == null ? null : opcTagIDNode.asText();

            final JsonNode opcTagReadingIDNode = jsonNode.findValue("OPCTagReadingID");
            final String opcTagReadingID = opcTagReadingIDNode == null ? null : opcTagReadingIDNode.asText();

            final JsonNode qualityNode = jsonNode.findValue("Quality");
            final String quality = qualityNode == null ? null : qualityNode.asText();

            final JsonNode timestampNode = jsonNode.findValue("Timestamp");
            final String timestamp = timestampNode == null ? null : timestampNode.asText();

            final JsonNode valueNode = jsonNode.findValue("Value");
            final String value = valueNode == null ? null : valueNode.asText();

            final JsonNode idNode = jsonNode.findValue("ID");
            final String id = idNode == null ? null : idNode.asText();

            final JsonNode uniqueIDNode = jsonNode.findValue("UniqueID");
            final String uniqueID = uniqueIDNode == null ? null : uniqueId(uniqueIDNode);

            return new Signal(uniqueID, id, value, timestamp, quality, opcTagReadingID, opcTagID, proxiedTypeName,
                    extendedProperties);
        }
    }

    private static String uniqueId(final JsonNode uniqueIDNode) {
        return uniqueIDNode.isNull() ? null : uniqueIDNode.asText();
    }
}
