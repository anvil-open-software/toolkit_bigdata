package com.dematic.labs.toolkit_bigdata.simulators.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.ProducerConfig;

public abstract class ProducerConfiguration {
    public static abstract class Builder<T extends Builder<T>> {
        // producer keys
        private static final String PRODUCER_ID = "producer.id";
        private static final String DURATION_IN_MINUTES = "producer.durationInMinutes";

        // loads all the producer configurations and the reference configuration
        private final Config config = ConfigFactory.load();
        // shared producer values
        private final String id;
        private final long durationInMinutes;
        // shared kafka values
        private final String bootstrapServers;
        private final String topics;
        private final String keySerializer;
        private final String valueSerializer;
        private final String acks;
        private final int retries;

        protected Builder() {
            // all values come from external configuration
            id = config.getString(PRODUCER_ID);
            durationInMinutes = config.getLong(DURATION_IN_MINUTES);
            // kafka configuration, using kafka keys when possible
            bootstrapServers = config.getString(String.format("kafka.%s", ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            topics = config.getString("kafka.topics");
            keySerializer = config.getString(String.format("kafka.%s", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
            valueSerializer = config.getString(String.format("kafka.%s", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
            acks = config.getString(String.format("kafka.%s", ProducerConfig.ACKS_CONFIG));
            retries = config.getInt(String.format("kafka.%s", ProducerConfig.RETRIES_CONFIG));
        }

        protected Config getConfig() {
            return config;
        }

        public abstract T getThis();
    }

    private final String id;
    private long durationInMinutes;
    // shared kafka values
    private final String bootstrapServers;
    private final String topics;
    private final String keySerializer;
    private final String valueSerializer;
    private final String acks;
    private final int retries;

    protected ProducerConfiguration(final Builder builder) {
        id = builder.id;
        durationInMinutes = builder.durationInMinutes;
        bootstrapServers = builder.bootstrapServers;
        topics = builder.topics;
        keySerializer = builder.keySerializer;
        valueSerializer = builder.valueSerializer;
        acks = builder.acks;
        retries = builder.retries;
    }

    public String getId() {
        return id;
    }

    public long getDurationInMinutes() {
        return durationInMinutes;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopics() {
        return topics;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public String getAcks() {
        return acks;
    }

    public int getRetries() {
        return retries;
    }
}

