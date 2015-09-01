package com.dematic.labs.toolkit.aws;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.ImmutableSet;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

    private static final Set<String> RETRYABLE_ERR_CODES = ImmutableSet.of("ProvisionedThroughputExceededException",
            "InternalFailure", "ServiceUnavailable");

    private static final AtomicLong TOTAL_EVENTS = new AtomicLong(0);
    private static final AtomicLong SYSTEM_ERROR = new AtomicLong(0);
    private static final AtomicLong KINESIS_ERROR = new AtomicLong(0);

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
        final ForkJoinPool forkJoinPool = new ForkJoinPool(parallelism, ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null, true);
        try {
            forkJoinPool.submit(() -> {
                FixedBatchSpliterator.withBatchSize(LongStream.range(1, numberOfEvents + 1).boxed(), streamChunkingSize).
                        parallel().
                        forEach(event -> {
                            // 1) generate batched events and dispatch request
                            final List<PutRecordsRequestEntry> putRecordsRequestEntries =
                                    generatePutRecordsRequestEntries(kinesisRecordsPerRequest, nodeSize, orderSize);
                            dispatch(amazonKinesisClient, kinesisInputStream, putRecordsRequestEntries, 20);
                        });
            }).get();
        } catch (final InterruptedException | ExecutionException ex) {
            LOGGER.error("unexpected exception:", ex);
        } finally {
            forkJoinPool.shutdownNow();
        }
    }

    private static void dispatch(final AmazonKinesisAsyncClient amazonKinesisClient, final String stream,
                                 final List<PutRecordsRequestEntry> putRecordsRequestEntries, final int numberOfTries) {
        TOTAL_EVENTS.getAndAdd(putRecordsRequestEntries.size());
        int count = 0;
        final PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(stream);
        putRecordsRequest.setRecords(putRecordsRequestEntries);
        PutRecordsResult putRecordsResult = null;

        do {
            try {
                final Future<PutRecordsResult> futurePutRecordsResult =
                        amazonKinesisClient.putRecordsAsync(putRecordsRequest);
                putRecordsResult = futurePutRecordsResult.get();
                // deal with any type of system, connection exception, etc...
            } catch (final Throwable any) {
                LOGGER.error("unexpected exception dispatching to kinesis, trying again : count = {}", count, any);
            } finally {
                count++;
            }

            // deal with kinesis exception, provision...
            if (putRecordsResult == null) {
                continue;
            }

            if (putRecordsResult.getFailedRecordCount() > 0) {
                LOGGER.debug("failed pushed events: {}, trying again : count = {}",
                        putRecordsResult.getFailedRecordCount(), count);
                final List<PutRecordsResultEntry> failed = putRecordsResult.getRecords();
                // retrieve the failed events
                final List<PutRecordsRequestEntry> retries = IntStream.range(0, failed.size())
                        .mapToObj(i -> {
                            final PutRecordsRequestEntry retry = putRecordsRequestEntries.get(i);
                            final String errorCode = failed.get(i).getErrorCode();
                            // only retry accepeted error codes
                            if (errorCode != null && RETRYABLE_ERR_CODES.contains(errorCode)) {
                                LOGGER.error("known failure: >{}<", failed.get(i).toString());
                                return Optional.of(retry);
                            } else {
                                // these are unknown error, we will try them anyway
                                LOGGER.debug("unknown failure: no error code >{}<", failed.get(i).toString());
                                return Optional.of(retry);
                            }
                        })
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
                // set the failed request to be tried again
                putRecordsRequest.setRecords(retries);
            }
        } while ((putRecordsResult == null && count > 0 && count < numberOfTries) ||
                (putRecordsResult != null && putRecordsResult.getFailedRecordCount() > 0 && count < numberOfTries));

        // failed to dispatch because of issues associated with not being able to connect to kineses
        if (putRecordsResult == null) {
            final int systemError = putRecordsRequest.getRecords().size();
            SYSTEM_ERROR.getAndAdd(systemError);
            LOGGER.error("unable to dispatch: {} events (system error)", systemError);
        }

        if (putRecordsResult != null && putRecordsResult.getFailedRecordCount() > 0) {
            final int failedRecordCount = putRecordsResult.getFailedRecordCount();
            KINESIS_ERROR.getAndAdd(failedRecordCount);
            LOGGER.error("unable to dispatch: {} events (kinesis and unknown error)", failedRecordCount);
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
            LOGGER.info("Total Events ATTEMPTED: {}", TOTAL_EVENTS.get());
            LOGGER.info("Total Events FAILED: {}", SYSTEM_ERROR.get() + KINESIS_ERROR.get());
            LOGGER.info("Total System Events FAILED: {}", SYSTEM_ERROR.get());
            LOGGER.info("Total Kinesis Events FAILED: {}", KINESIS_ERROR.get());

            if (amazonKinesisClient != null) {
                amazonKinesisClient.shutdown();
            }
            System.exit(0);
        }
    }
}
