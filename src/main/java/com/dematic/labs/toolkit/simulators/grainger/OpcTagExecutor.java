package com.dematic.labs.toolkit.simulators.grainger;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.dematic.labs.toolkit.aws.kinesis.KinesisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.dematic.labs.toolkit.aws.Connections.getAmazonKinesisClient;

public final class OpcTagExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpcTagExecutor.class);

    private OpcTagExecutor(){
    }

    private static void dispatchOpcTags(final Path dir, final String streamType, final String streamEndpoint,
                                        final String streamName) {
        // read files and send opc tags to stream
        try {
            Files.walk(dir).parallel().forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    try {
                        final byte[] signal = Files.readAllBytes(filePath);
                        //todo: deal with dispatching to different streams, kinesis vs kafka
                        final AmazonKinesisClient amazonKinesisClient = getAmazonKinesisClient(streamEndpoint);
                        KinesisClient.dispatchSignalToKinesisWithRetries(amazonKinesisClient, streamName, signal, 3);
                    } catch (final IOException ioe) {
                        LOGGER.error("can't read file >{}< moving to next file", filePath);
                    }
                }
            });
        } catch (final IOException ioe) {
            throw new IllegalArgumentException(String.format("Unexpected Exception : can't read files from >%s<", dir));
        }
    }

    public static void main(final String[] args) {
        if (args == null || args.length < 4) {
            throw new IllegalArgumentException("OpcTagExecutor: Please ensure the following are set: opcTagDir, " +
                    "streamType='kinesis or kafka', streamName, streamEndpoint");
        }
        final Path dir = Paths.get(args[0]);
        final String streamType = args[1];
        final String streamEndpoint = args[2];
        final String streamName = args[3];

        dispatchOpcTags(dir, streamType, streamEndpoint, streamName);
    }
}
