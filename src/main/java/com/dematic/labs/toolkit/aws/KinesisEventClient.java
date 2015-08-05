package com.dematic.labs.toolkit.aws;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.dematic.labs.toolkit.communication.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import static com.dematic.labs.toolkit.aws.Connections.getAmazonKinesisClient;
import static com.dematic.labs.toolkit.communication.EventUtils.eventToJsonByteArray;
import static com.dematic.labs.toolkit.communication.EventUtils.generateEvents;

/**
 * Client will push generated events to a kinesis stream.
 * <p/>
 * <p/>
 * NOTE: does not handle any failures, retries, .... that is, will just log and continue
 */
public class KinesisEventClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventClient.class);

    private KinesisEventClient() {
    }

    public static void pushEventsToKinesis(final String kinesisEndpoint, final String kinesisInputStream,
                                           final List<Event> events) {
        final AmazonKinesisClient amazonKinesisClient = getAmazonKinesisClient(kinesisEndpoint);
        //todo: think about join pool and batch size, make configurable
        final ForkJoinPool forkJoinPool = new ForkJoinPool(100);

        try {
            forkJoinPool.submit(() -> FixedBatchSpliterator.withBatchSize(events.stream(), 1000).
                    parallel().
                    forEach(event -> {
                        final PutRecordRequest putRecordRequest = new PutRecordRequest();
                        putRecordRequest.setStreamName(kinesisInputStream);
                        try {
                            putRecordRequest.setData(ByteBuffer.wrap(eventToJsonByteArray(event)));
                            putRecordRequest.setPartitionKey(randomPartitionKey());
                            final PutRecordResult putRecordResult =
                                    amazonKinesisClient.putRecord(putRecordRequest);
                            LOGGER.debug("pushed event >{}< : status: {}", event.getEventId(), putRecordResult.toString());
                        } catch (final IOException ioe) {
                            LOGGER.error("unable to push event >{}< to the kinesis stream", event, ioe);
                        }
                    })).get();
        } catch (final InterruptedException | ExecutionException ex) {
            LOGGER.error("unexpected exception:", ex);
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
    public static String randomPartitionKey() {
        return new BigInteger(128, RANDOM).toString(10);
    }
}
