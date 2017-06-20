package com.dematic.labs.toolkit_bigdata.simulators.configuration;

import com.dematic.labs.toolkit_bigdata.simulators.configuration.grainger.OpcTagReaderConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class ProducerConfigurationTest {
    @Test
    public void opcTagReadingExecutorConfiguration() {
        // configuration comes from the application.conf for the driver
        final OpcTagReaderConfiguration config = new OpcTagReaderConfiguration.Builder().build();

        // from application.conf
        Assert.assertEquals("opcTagExecutor", config.getId());
        Assert.assertEquals("test", config.getTopics());
        Assert.assertEquals(10, config.getRetries());

        // from reference.conf
        Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer",
                config.getKeySerializer());
        Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer",
                config.getValueSerializer());
        Assert.assertEquals("all", config.getAcks());
    }
}
