package com.dematic.labs.toolkit;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SystemPropertyRule extends ExternalResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemPropertyRule.class);
    // load system properties from the junit.properties file
    @Override
    protected void before() throws Throwable {
        final String junitPropertiesPath = String.format("%s/.m2/junit.properties", System.getProperty("user.home"));
        final Stream<String> propertiesStream = Files.lines(Paths.get(junitPropertiesPath));

        final List<String> properties = propertiesStream.collect(Collectors.toList());
        for (final String property : properties) {
            final String[] split = property.split("=");
            final String propertyKey = split[0];
            if (!propertyKey.isEmpty() && !propertyKey.startsWith("#") && !propertyKey.startsWith("nexus")) {
                final String propertyValue = split[1];
                // special case for kinesisInputStream
                if (propertyKey.equals("kinesisInputStream")) {
                    final String kinesisInputStream = String.format("%s_stream", System.getProperty("user.name"));
                    System.setProperty(propertyKey, kinesisInputStream);
                    LOGGER.info("created kinesis stream >{}<", kinesisInputStream);
                } else {
                    // add to system properties
                    System.setProperty(propertyKey, propertyValue);
                }
            }
        }
    }
}
