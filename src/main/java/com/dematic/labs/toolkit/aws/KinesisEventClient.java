package com.dematic.labs.toolkit.aws;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.Metric;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static com.dematic.labs.toolkit.communication.EventUtils.eventToJsonByteArray;
import static com.dematic.labs.toolkit.communication.EventUtils.generateEvents;

/**
 * Client will push generated events to a kinesis stream.
 * <p/>
 * <p/>
 * NOTE: does not handle any failures, that is, will just log and continue
 */
public class KinesisEventClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventClient.class);

    private KinesisEventClient() {
    }

    public static void pushEventsToKinesis(final String kinesisEndpoint, final String kinesisInputStream,
                                           final List<Event> events) {
        final KinesisProducerConfiguration kinesisProducerConfiguration = new KinesisProducerConfiguration();
        // get the region from the URL
        kinesisProducerConfiguration.setRegion(RegionUtils.getRegionByEndpoint(kinesisEndpoint).getName());
        kinesisProducerConfiguration.setRateLimit(9223372036854775807L);
        // max connection is 128
        kinesisProducerConfiguration.setMaxConnections(128);
        final KinesisProducer kinesisProducer = new KinesisProducer(kinesisProducerConfiguration);

        events.stream()
                .parallel()
                .forEach(event -> {
                            try {
                                final ListenableFuture<UserRecordResult> userRecordResult =
                                        kinesisProducer.addUserRecord(kinesisInputStream, randomExplicitHashKey(),
                                                ByteBuffer.wrap(eventToJsonByteArray(event)));
                                LOGGER.debug("event sent to shard >{}<", userRecordResult.get().getShardId());
                            } catch (final IOException | InterruptedException | ExecutionException ioe) {
                                LOGGER.error("unable to push event >{}< to the kinesis stream", event, ioe);
                            }
                        }
                );
        try {
            kinesisProducer.flushSync();
            final List<Metric> metrics = kinesisProducer.getMetrics();
            for (final Metric metric : metrics) {
                LOGGER.info(metric.toString());
            }
        } catch (final InterruptedException | ExecutionException ioe) {
            LOGGER.error("unable to get metrics", ioe);
        } finally {
            kinesisProducer.destroy();
        }
    }

    public static void main(final String[] args) {
        if (args == null || args.length != 5) {
            throw new IllegalArgumentException(
                    "ensure all the following are set {kinesisEndpoint, kinesisInputStream, numberOfEvents, nodeSize, " +
                            "orderSize");
        }
        final String kinesisEndpoint = args[0];
        final String kinesisInputStream = args[1];

        final long numberOfEvents = Long.valueOf(args[2]);
        final int nodeSize = Integer.valueOf(args[3]);
        final int orderSize = Integer.valueOf(args[4]);

        //generate events and push to Kinesis
        pushEventsToKinesis(kinesisEndpoint, kinesisInputStream, generateEvents(numberOfEvents, nodeSize, orderSize));
    }

    private static final Random RANDOM = new Random();

    /**
     * @return A random unsigned 128-bit int converted to a decimal string.
     */
    public static String randomExplicitHashKey() {
        return new BigInteger(128, RANDOM).toString(10);
    }
}
