/*
 *  Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

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

    @Override
    public String toString() {
        return "MinimalProducerConfiguration{} " + super.toString();
    }
}
