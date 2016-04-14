package com.dematic.labs.toolkit.communication;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class SignalTest {
    @Test
    public void deserializeSerialize() throws IOException, URISyntaxException {
        //read file into stream, try-with-resources
        try (final Stream<String> stream =
                     Files.lines(Paths.get(this.getClass().getResource("/grainger/raw/14260000104630").toURI()))) {
            stream.forEach(raw -> {
                try {
                    // deserialize
                    final Signal signal = SignalUtils.jsonToSignal(raw);
                    // serialize
                    final String json = SignalUtils.signalToJson(signal);
                    final ObjectMapper mapper = new ObjectMapper();
                    // assert ==
                    final JsonNode tree1 = mapper.readTree(raw);
                    final JsonNode tree2 = mapper.readTree(json);
                    Assert.assertEquals(tree1, tree2);
                } catch (final IOException ignore) {
                }
            });
        }
    }
}
