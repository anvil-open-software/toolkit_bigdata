package com.dematic.labs.toolkit.helpers.test_util;

import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.lang.System.clearProperty;
import static java.lang.System.getProperty;
import static java.lang.System.setProperty;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Paths.get;


/**
 * load system properties from the junit.properties file
 */
public final class SystemPropertyRule extends ExternalResource {
    private final Map<String, String> previousProperties = new HashMap<>();

    @Override
    protected void before() throws IOException {
        final Path junitPropertiesPath = get(getProperty("user.home"), ".m2", "junit.properties");
        try (final InputStream inputStream = newInputStream(junitPropertiesPath)) {
            final Properties junitProperties = new Properties();
            junitProperties.load(inputStream);
            for (final String propertyKey : junitProperties.stringPropertyNames()) {
                if (propertyKey.startsWith("nexus")) {
                    continue;
                }
                put(propertyKey, junitProperties.getProperty(propertyKey));
            }
        }
    }

    public void put(final String propertyKey, final String propertyValue) {
        previousProperties.put(propertyKey, setProperty(propertyKey, propertyValue));
    }

    @Override
    protected void after() {
        for (final Map.Entry<String, String> property : previousProperties.entrySet()) {
            if (property.getValue() == null) {
                clearProperty(property.getKey());
            } else {
                setProperty(property.getKey(), property.getValue());
            }
        }
    }
}
