/*
 *  Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.toolkit_bigdata.simulators.configuration.grainger;

import com.dematic.labs.toolkit_bigdata.simulators.configuration.ProducerConfiguration;

public final class OpcTagReaderConfiguration extends ProducerConfiguration {
    public static class Builder extends ProducerConfiguration.Builder<Builder> {
        // OpcTagReaderConfiguration producer keys
        private static final String OPC_TAG_RANGE_MIN = "producer.opcTagRangeMin";
        private static final String OPC_TAG_RANGE_MAX = "producer.opcTagRangeMax";
        private static final String MAX_SIGNALS_PER_MINUTE_PER_OPC_TAG = "producer.maxSignalsPerMinutePerOpcTag";

        private final int opcTagRangeMin;
        private final int opcTagRangeMax;
        private final int maxSignalsPerMinutePerOpcTag;

        public Builder() {
            // all values come from external configuration associated to this producer
            opcTagRangeMin = getConfig().getInt(OPC_TAG_RANGE_MIN);
            opcTagRangeMax = getConfig().getInt(OPC_TAG_RANGE_MAX);
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

    private final int opcTagRangeMin;
    private final int opcTagRangeMax;
    private final int maxSignalsPerMinutePerOpcTag;

    OpcTagReaderConfiguration(final Builder builder) {
        super(builder);
        opcTagRangeMin = builder.opcTagRangeMin;
        opcTagRangeMax = builder.opcTagRangeMax;
        maxSignalsPerMinutePerOpcTag = builder.maxSignalsPerMinutePerOpcTag;
    }

    public int getOpcTagRangeMin() {
        return opcTagRangeMin;
    }

    public int getOpcTagRangeMax() {
        return opcTagRangeMax;
    }

    public int getMaxSignalsPerMinutePerOpcTag() {
        return maxSignalsPerMinutePerOpcTag;
    }
}
