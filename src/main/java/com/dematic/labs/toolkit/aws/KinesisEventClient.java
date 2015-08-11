package com.dematic.labs.toolkit.aws;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.dematic.labs.toolkit.communication.Event;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.dematic.labs.toolkit.aws.Connections.getAmazonAsyncKinesisClient;
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
        events.stream()
                .parallel()
                .forEach(event -> {
                    final PutRecordRequest putRecordRequest = new PutRecordRequest();
                    putRecordRequest.setStreamName(kinesisInputStream);
                    try {
                        putRecordRequest.setData(ByteBuffer.wrap(eventToJsonByteArray(event)));
                        // group by partitionKey
                        putRecordRequest.setPartitionKey(randomPartitionKey());
                        final PutRecordResult putRecordResult =
                                amazonKinesisClient.putRecord(putRecordRequest);
                        LOGGER.info("pushed event >{}< to shard>{}<", event.getEventId(), putRecordResult.getShardId());
                    } catch (final IOException ioe) {
                        LOGGER.error("unable to push event >{}< to the kinesis stream", event, ioe);
                    }
                });
    }

    private static void pushEventsToKinesisAsync(final AmazonKinesisAsyncClient amazonKinesisClient,
                                                 final String kinesisInputStream, final long numberOfEvents,
                                                 final int nodeSize, final int orderSize,
                                                 final int kinesisRecordsPerRequest, final int streamChunkingSize,
                                                 final int parallelism) {
        final ForkJoinPool forkJoinPool = new ForkJoinPool(parallelism);

        try {
            forkJoinPool.submit(() -> {
                FixedBatchSpliterator.withBatchSize(LongStream.range(1, numberOfEvents + 1).boxed(), streamChunkingSize).
                        parallel().
                        forEach(event -> {
                            final PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
                            putRecordsRequest.setStreamName(kinesisInputStream);
                            final List<PutRecordsRequestEntry> putRecordsRequestEntries =
                                    generatePutRecordsRequestEntries(kinesisRecordsPerRequest, nodeSize, orderSize);
                            putRecordsRequest.setRecords(putRecordsRequestEntries);

                            final Future<PutRecordsResult> futurePutRecordsResult =
                                    amazonKinesisClient.putRecordsAsync(putRecordsRequest);
                            PutRecordsResult putRecordsResult = null;
                            try {
                                putRecordsResult = futurePutRecordsResult.get(1, TimeUnit.MINUTES);
                            } catch (final InterruptedException | ExecutionException | TimeoutException resultEx) {
                                LOGGER.error("unable to retrieve kinesis results in under a 1 minute", resultEx);
                            }
                            if (putRecordsResult != null && putRecordsResult.getFailedRecordCount() > 0) {
                                LOGGER.error("failed pushed events: {}", putRecordsResult.getFailedRecordCount());
                                final List<PutRecordsResultEntry> records = putRecordsResult.getRecords();
                                records.stream().parallel().forEach(record -> {
                                    if (record.getErrorCode() != null) {
                                        LOGGER.error("reason: {} : {} --> {}", record.getErrorCode(),
                                                record.getErrorMessage(), record.getShardId());
                                    }
                                });
                            }
                        });
            }).get();
        } catch (final InterruptedException | ExecutionException ex) {
            LOGGER.error("unexpected exception:", ex);
        } finally {
            forkJoinPool.shutdownNow();
            amazonKinesisClient.shutdown();
        }
    }

    private static void pushEventsToKinesisAsync(final AmazonKinesisAsyncClient amazonKinesisClient,
                                                 final String kinesisInputStream, final int nodeSize,
                                                 final int orderSize, final TimeUnit unit, final long time,
                                                 final int kinesisRecordsPerRequest, final int streamChunkingSize,
                                                 final int parallelism) {
        try {
            Awaitility.waitAtMost(time, unit).until(() -> {
                        //noinspection InfiniteLoopStatement
                        for (;;) {
                            //todo: picked a very large number of events to slow pushing down, need to be able to
                            // todo: throttle this
                            pushEventsToKinesisAsync(amazonKinesisClient, kinesisInputStream, 1000000, nodeSize, orderSize,
                                    kinesisRecordsPerRequest, streamChunkingSize, parallelism);
                        }
                    }
            );
        } catch (final ConditionTimeoutException ignore) {
            // we've reached the maximum time allocated
            LOGGER.info("finished pushing events for {} {}", time, unit);
        }
    }

    private static List<PutRecordsRequestEntry> generatePutRecordsRequestEntries(final int numberOfEvents,
                                                                                 final int nodeSize,
                                                                                 final int orderSize) {
        return generateEvents(numberOfEvents, nodeSize, orderSize).parallelStream().map(event -> {
            final PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            try {
                putRecordsRequestEntry.setData(ByteBuffer.wrap(eventToJsonByteArray(event)));
            } catch (final IOException ignore) {
                LOGGER.error("unable to transform >{}< into a byte buffer", event);
            }
            putRecordsRequestEntry.setPartitionKey(randomPartitionKey());
            return putRecordsRequestEntry;
        }).collect(Collectors.toList());
    }

    private static final Random RANDOM = new Random();

    /**
     * @return A random unsigned 128-bit int converted to a decimal string.
     */
    public static String randomPartitionKey() {
        return new BigInteger(128, RANDOM).toString(10);
    }

    public static void main(final String[] args) {
        AmazonKinesisAsyncClient amazonKinesisClient = null;

        //noinspection finally
        try {
            if (args != null && args.length == 8) {
                final String kinesisEndpoint = args[0];
                final String kinesisInputStream = args[1];
                final long numberOfEvents = Long.valueOf(args[2]);
                final int nodeSize = Integer.valueOf(args[3]);
                final int orderSize = Integer.valueOf(args[4]);
                final int kinesisRecordsPerRequest = Integer.valueOf(args[5]);
                final int streamChunkSize = Integer.valueOf(args[6]);
                final int parallelism = Integer.valueOf(args[7]);
                amazonKinesisClient = getAmazonAsyncKinesisClient(kinesisEndpoint, parallelism);
                //generate events and push to Kinesis
                pushEventsToKinesisAsync(amazonKinesisClient, kinesisInputStream, numberOfEvents, nodeSize, orderSize,
                        kinesisRecordsPerRequest, streamChunkSize, parallelism);
            } else if (args != null && args.length == 9) {
                final String kinesisEndpoint = args[0];
                final String kinesisInputStream = args[1];
                final int nodeSize = Integer.valueOf(args[2]);
                final int orderSize = Integer.valueOf(args[3]);
                final TimeUnit unit = TimeUnit.valueOf(args[4]);
                final long time = Long.valueOf(args[5]);
                final int kinesisRecordsPerRequest = Integer.valueOf(args[6]);
                final int streamChunkSize = Integer.valueOf(args[7]);
                final int parallelism = Integer.valueOf(args[8]);

                amazonKinesisClient = getAmazonAsyncKinesisClient(kinesisEndpoint, parallelism);
                //generate events and push to Kinesis
                pushEventsToKinesisAsync(amazonKinesisClient, kinesisInputStream, nodeSize, orderSize, unit, time,
                        kinesisRecordsPerRequest, streamChunkSize, parallelism);
            } else {
                throw new IllegalArgumentException(
                        "ensure all the following are set {kinesisEndpoint, kinesisInputStream, numberOfEvents, nodeSize, " +
                                "orderSize, kinesisRecordsPerRequest, streamChunkSize, levelOfParallelism} or " +
                                "{kinesisEndpoint, kinesisInputStream, nodeSize, orderSize, timeUnit, time, " +
                                "kinesisRecordsPerRequest, streamChunkSize, levelOfParallelism}");
            }
        } finally {
            if (amazonKinesisClient != null) {
                amazonKinesisClient.shutdown();
            }
            System.exit(0);
        }
    }
}
