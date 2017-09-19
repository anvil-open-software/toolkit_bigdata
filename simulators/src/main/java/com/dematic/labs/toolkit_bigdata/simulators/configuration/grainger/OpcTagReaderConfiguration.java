package com.dematic.labs.toolkit_bigdata.simulators.configuration.grainger;

import com.dematic.labs.toolkit_bigdata.simulators.configuration.ProducerConfiguration;

public final class OpcTagReaderConfiguration extends ProducerConfiguration {
    public static class Builder extends ProducerConfiguration.Builder<Builder> {
        private static final String MAX_SIGNALS_PER_MINUTE_PER_OPC_TAG = "producer.maxSignalsPerMinutePerOpcTag";

        private final int maxSignalsPerMinutePerOpcTag;

        public Builder() {
            // all values come from external configuration associated to this producer
            maxSignalsPerMinutePerOpcTag = getConfig().getInt(MAX_SIGNALS_PER_MINUTE_PER_OPC_TAG);
        }

        @Override
        public Builder getThis() {
            return this;
        }

        public OpcTagReaderConfiguration build() {
            return new OpcTagReaderConfiguration(this);
        }
    }

    private final int maxSignalsPerMinutePerOpcTag;

    OpcTagReaderConfiguration(final Builder builder) {
        super(builder);
        maxSignalsPerMinutePerOpcTag = builder.maxSignalsPerMinutePerOpcTag;
    }

    public int getMaxSignalsPerMinutePerOpcTag() {
        return maxSignalsPerMinutePerOpcTag;
    }

    @Override
    public String toString() {
        return "OpcTagReaderConfiguration{" +
                "maxSignalsPerMinutePerOpcTag=" + maxSignalsPerMinutePerOpcTag +
                "} " + super.toString();
    }
}
