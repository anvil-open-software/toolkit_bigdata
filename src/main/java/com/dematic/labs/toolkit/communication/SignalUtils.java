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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
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

    public static byte[] signalToJsonByteArray(final Signal signal) throws IOException {
        return signalToJson(signal).getBytes(Charset.defaultCharset());
    }

    private final static class SignalSerializer extends JsonSerializer<Signal> {
        @Override
        public void serialize(final Signal signal, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializers)
                throws IOException {
            try {
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
                jsonGenerator.writeNumberField("OPCTagID", signal.getOpcTagId());
                jsonGenerator.writeStringField("OPCTagReadingID", signal.getOpcTagReadingId());
                jsonGenerator.writeStringField("Quality", signal.getQuality());
                jsonGenerator.writeStringField("Timestamp", signal.getTimestamp());
                jsonGenerator.writeStringField("Value", signal.getValue());
                jsonGenerator.writeStringField("ID", signal.getId());
                jsonGenerator.writeStringField("UniqueID", signal.getUniqueId());
            } finally {
                jsonGenerator.writeEndObject();
                jsonGenerator.close();
            }
        }
    }

    private final static class SignalDeserializer extends JsonDeserializer<Signal> {
        @Override
        public Signal deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException {
            final ObjectCodec codec = jp.getCodec();
            final JsonNode jsonNode = codec.readTree(jp);

            final JsonNode extendedPropertiesNode = jsonNode.findValue("ExtendedProperties");
            final List<String> extendedProperties = new ArrayList<>();
            if(extendedPropertiesNode != null) {
                for (final JsonNode next : extendedPropertiesNode) {
                    final String property = next.isNull() ? null : next.asText();
                    extendedProperties.add(property);
                }
            }

            final JsonNode proxiedTypeNameNode = jsonNode.findValue("ProxiedTypeName");
            final String proxiedTypeName = proxiedTypeNameNode == null ? null : proxiedTypeNameNode.asText();

            final JsonNode opcTagIDNode = jsonNode.findValue("OPCTagID");
            final long opcTagID = opcTagIDNode == null ? 0 : opcTagIDNode.asLong();

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
            final String uniqueID = uniqueIDNode == null ? null : uniqueIDNode.asText();

            return new Signal(uniqueID, id, value, timestamp, quality, opcTagReadingID, opcTagID, proxiedTypeName,
                    extendedProperties);
        }
    }
}
