package com.dematic.labs.toolkit.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

public final class Connections {
    // key is for the partition
    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    // value is the data sent to kafka
    private static final String BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";

    public static KafkaProducer<String, byte[]> getKafkaProducer(final String serverIpAddress) {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverIpAddress);
        // default properties
        properties.put(ProducerConfig.ACKS_CONFIG, "all"); // guaranteed delivery
        properties.put(ProducerConfig.RETRIES_CONFIG, "3"); // if delivery ordered is needed, then need to re-visit retries
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BYTE_ARRAY_SERIALIZER);
        return new KafkaProducer<>(properties);
    }
}
