package com.dematic.labs.toolkit_bigdata.simulators.configuration;

import org.junit.Assert;
import org.junit.Test;

public class ProducerConfigurationTest {
    @Test
    public void opcTagExecutorConfiguration() {
        // from application.conf
        Assert.assertEquals("opcTagExecutor", ProducerConfiguration.getProducerId());
        Assert.assertEquals("test", ProducerConfiguration.getTopics());
        Assert.assertEquals(10, ProducerConfiguration.getRetries());

        // from reference.conf
        Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer", ProducerConfiguration.getKeySerializer());
        Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer", ProducerConfiguration.getValueSerializer());
        Assert.assertEquals("all", ProducerConfiguration.getAcks());
    }
}
