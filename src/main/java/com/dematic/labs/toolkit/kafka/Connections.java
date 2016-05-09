package com.dematic.labs.toolkit.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;

public final class Connections {
    // key is for the partition
    private static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    // value is the data sent to kafka
    private static final String BYTE_ARRAY_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    public static KafkaProducer<String, byte[]> getKafkaProducer(final String serverIpAddress) {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", serverIpAddress);
        // default properties
        properties.put("acks", "all"); // guaranteed delivery
        properties.put("retries", "3"); // if delivery ordered is needed, then need to re-visit retries
        properties.put("key.deserializer", STRING_DESERIALIZER);
        properties.put("value.deserializer", BYTE_ARRAY_DESERIALIZER);
        return new KafkaProducer<>(properties);
    }
}
