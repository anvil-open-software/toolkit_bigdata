package com.dematic.labs.toolkit_bigdata.simulators.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.ProducerConfig;

final class ProducerConfiguration {
    // producer keys
    private static final String PRODUCER_ID = "producer.id";
    private static final String DURATION_IN_MINUTES = "producer.durationInMinutes";

    // loads all the producer configurations and the reference configuration
    private static Config config = ConfigFactory.load();

    private ProducerConfiguration() {
    }

    // producer configuration
    static String getProducerId() {
        return config.getString(PRODUCER_ID);
    }

    static long getDurationInMinutes() {
        return config.getLong(DURATION_IN_MINUTES);
    }

    // kafka configuration, using kafka keys when possible
    static String getBootstrapServers() {
        return config.getString(String.format("kafka.%s", ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    static String getTopics() {
        return config.getString("kafka.topics");
    }

    static String getKeySerializer() {
        return config.getString(String.format("kafka.%s", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    }

    static String getValueSerializer() {
        return config.getString(String.format("kafka.%s", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    static String getAcks() {
        return config.getString(String.format("kafka.%s", ProducerConfig.ACKS_CONFIG));
    }

    static String getRetries() {
        return config.getString(String.format("kafka.%s", ProducerConfig.RETRIES_CONFIG));
    }
}

