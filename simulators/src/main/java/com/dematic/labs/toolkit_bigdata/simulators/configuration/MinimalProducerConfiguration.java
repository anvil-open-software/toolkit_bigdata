package com.dematic.labs.toolkit_bigdata.simulators.configuration;

public final class MinimalProducerConfiguration extends ProducerConfiguration {
    public static class Builder extends ProducerConfiguration.Builder<Builder> {
        @Override
        public Builder getThis() {
            return this;
        }

        public MinimalProducerConfiguration build() {
            return new MinimalProducerConfiguration(this);
        }
    }

    MinimalProducerConfiguration(final Builder builder) {
        super(builder);
    }
}
