package com.dematic.labs.toolkit.communication;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
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

    public static Signal jsonByteArrayToSignal(final byte[] json) throws IOException {
        return objectMapper.readValue(json, Signal.class);
    }

    public static String signalToJson(final Signal signal) throws IOException {
        return objectMapper.writeValueAsString(signal);
    }

    public static Date toJavaUtilDateFromZonedDateTime(final ZonedDateTime zonedDateTime) {
        final Instant instant = zonedDateTime.toInstant();
        final long millisecondsSinceEpoch = instant.toEpochMilli();  // Data-loss, going from nanosecond resolution to milliseconds.
        return new Date(millisecondsSinceEpoch);
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
                jsonGenerator.writeNumberField("OPCTagID", signal.getOpcTagId());
                jsonGenerator.writeNumberField("OPCTagReadingID", signal.getOpcTagReadingId());
                jsonGenerator.writeNumberField("Quality", signal.getQuality());
                jsonGenerator.writeStringField("Timestamp", ZonedDateTime.ofInstant(signal.getTimestamp().toInstant(),
                        ZoneId.of("Z")).truncatedTo(ChronoUnit.NANOS).toString());
                jsonGenerator.writeNumberField("Value", signal.getValue());
                jsonGenerator.writeNumberField("ID", signal.getId());
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

    private final static class SignalDeserializer extends JsonDeserializer<Signal> {
        @Override
        public Signal deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException {
            final ObjectCodec codec = jp.getCodec();
            final JsonNode jsonNode = codec.readTree(jp);

            final JsonNode extendedPropertiesNode = jsonNode.findValue("ExtendedProperties");
            final List<String> extendedProperties = new ArrayList<>();
            if (extendedPropertiesNode != null) {
                for (final JsonNode next : extendedPropertiesNode) {
                    final String property = next.isNull() ? null : next.textValue();
                    extendedProperties.add(property);
                }
            }

            final JsonNode proxiedTypeNameNode = jsonNode.findValue("ProxiedTypeName");
            final String proxiedTypeName = proxiedTypeNameNode == null ? null : proxiedTypeNameNode.textValue();

            final JsonNode opcTagIDNode = jsonNode.findValue("OPCTagID");
            final Long opcTagID = opcTagIDNode == null ? null : opcTagIDNode.asLong();

            final JsonNode opcTagReadingIDNode = jsonNode.findValue("OPCTagReadingID");
            final Long opcTagReadingID = opcTagReadingIDNode == null ? null : opcTagReadingIDNode.asLong();

            final JsonNode qualityNode = jsonNode.findValue("Quality");
            final Long quality = qualityNode == null ? null : qualityNode.asLong();

            final JsonNode timestampNode = jsonNode.findValue("Timestamp");
            final Instant timestamp = timestampNode == null ? null : Instant.parse(timestampNode.textValue()).
                    truncatedTo(ChronoUnit.NANOS);

            final JsonNode valueNode = jsonNode.findValue("Value");
            final Long value = valueNode == null ? null : valueNode.asLong();

            final JsonNode idNode = jsonNode.findValue("ID");
            final Long id = idNode == null ? null : idNode.asLong();

            final JsonNode uniqueIDNode = jsonNode.findValue("UniqueID");
            final String uniqueID = uniqueIDNode == null ? null : uniqueId(uniqueIDNode);

            return new Signal(uniqueID, id, value, toDate(timestamp), quality, opcTagReadingID, opcTagID,
                    proxiedTypeName, extendedProperties);
        }
    }

    private static String uniqueId(final JsonNode uniqueIDNode) {
        return uniqueIDNode.isNull() ? null : uniqueIDNode.textValue();
    }

    private static Date toDate(final Instant instant) {
        if (instant == null) {
            return null;
        }
        return toJavaUtilDateFromZonedDateTime(ZonedDateTime.ofInstant(instant, ZoneId.of("Z")));
    }
}
