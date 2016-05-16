package com.dematic.labs.toolkit.simulators.grainger;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.dematic.labs.toolkit.aws.Connections.getAmazonKinesisClient;
import static com.dematic.labs.toolkit.aws.kinesis.KinesisClient.dispatchSignalToKinesisWithRetries;
import static com.dematic.labs.toolkit.kafka.Connections.getKafkaProducer;

public final class OpcTagReadingExecutorFromDir {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpcTagReadingExecutorFromDir.class);

    private OpcTagReadingExecutorFromDir() {
    }

    private static void dispatchOpcTagsToKinesis(final Path dir, final String streamEndpoint, final String streamName) {
        // read files and send opc tags to stream
        try {
            Files.walk(dir).parallel().forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    try {
                        final byte[] signal = Files.readAllBytes(filePath);
                        final AmazonKinesisClient amazonKinesisClient = getAmazonKinesisClient(streamEndpoint);
                        dispatchSignalToKinesisWithRetries(amazonKinesisClient, streamName, signal, 3);
                    } catch (final IOException ioe) {
                        LOGGER.error("can't read file >{}< moving to next file", filePath);
                    }
                }
            });
        } catch (final IOException ioe) {
            throw new IllegalArgumentException(String.format("Unexpected Exception : can't read files from >%s<", dir));
        }
    }

    private static void dispatchOpcTagsToKafka(final Path dir, final String streamEndpoint, final String streamName) {
        try (KafkaProducer<String, byte[]> kafkaProducer = getKafkaProducer(streamEndpoint)) {
            // read files and send opc tags to stream
            try {
                Files.walk(dir).parallel().forEach(filePath -> {
                    if (Files.isRegularFile(filePath)) {
                        try {
                            // streamName = kafka topic
                            final byte[] signal = Files.readAllBytes(filePath);
                            kafkaProducer.send(new ProducerRecord<>(streamName, signal));
                        } catch (final IOException ioe) {
                            LOGGER.error("can't read file >{}< moving to next file", filePath);
                        }
                    }
                });
            } catch (final IOException ioe) {
                throw new IllegalArgumentException(String.format("Unexpected Exception : can't read files from >%s<", dir));
            }
            kafkaProducer.flush();
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

        if ("kinesis".equals(streamType)) {
            dispatchOpcTagsToKinesis(dir, streamEndpoint, streamName);
        } else if ("kafka".equals(streamType)) {
            dispatchOpcTagsToKafka(dir, streamEndpoint, streamName);
        } else {
            throw new IllegalArgumentException(String.format("'%s' != 'kinesis or kafka'", streamType));
        }
    }
}
