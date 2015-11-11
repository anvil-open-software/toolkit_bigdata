package com.dematic.labs.toolkit.aws.kinesis;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.dematic.labs.toolkit.CountdownTimer;
import com.dematic.labs.toolkit.aws.EventRunParms;
import com.dematic.labs.toolkit.aws.FixedBatchSpliterator;
import com.dematic.labs.toolkit.aws.summary.KinesisEventSummary;
import com.dematic.labs.toolkit.aws.summary.KinesisEventSummaryPersister;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
 * <p>
 * <p>
 * NOTE: does not handle any failures, retries, .... that is, will just log and continue
 */
public class KinesisEventClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventClient.class);

    private static final Set<String> RETRYABLE_ERR_CODES = ImmutableSet.of("ProvisionedThroughputExceededException",
            "InternalFailure", "ServiceUnavailable");

    private static final AtomicLong TOTAL_EVENTS = new AtomicLong(0);
    private static final AtomicLong SUCCESS = new AtomicLong(0);
    private static final AtomicLong SYSTEM_ERROR = new AtomicLong(0);
    private static final AtomicLong KINESIS_ERROR = new AtomicLong(0);

    private KinesisEventClient() {
    }

    public static void dispatchEventsToKinesis(final String kinesisEndpoint, final String kinesisInputStream,
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
                        LOGGER.debug("pushed event >{}< to shard>{}<", event.getId(), putRecordResult.getShardId());
                    } catch (final IOException ioe) {
                        LOGGER.error("unable to push event >{}< to the kinesis stream", event, ioe);
                    }
                });
    }

    private static void dispatchEventsToKinesisAsync(final AmazonKinesisAsyncClient amazonKinesisClient,
                                                     final String kinesisInputStream, final long numberOfEvents,
                                                     final int kinesisRecordsPerRequest, final int streamChunkingSize,
                                                     final ForkJoinPool forkJoinPool) {
        try {
            forkJoinPool.submit(() -> {
                FixedBatchSpliterator.withBatchSize(LongStream.range(1, numberOfEvents + 1).boxed(), streamChunkingSize).
                        parallel().
                        forEach(event -> {
                            // 1) generate batched events and dispatch request
                            final List<PutRecordsRequestEntry> putRecordsRequestEntries =
                                    generatePutRecordsRequestEntries(kinesisRecordsPerRequest);
                            // todo: come back to retries and deal w duplicates
                            dispatch(amazonKinesisClient, kinesisInputStream, putRecordsRequestEntries, 0);
                        });
            }).get();
        } catch (final InterruptedException | ExecutionException ex) {
            LOGGER.error("unexpected exception:", ex);
        }
    }

    private static void dispatchEventsToKinesisAsync(final AmazonKinesisAsyncClient amazonKinesisClient,
                                                     final String kinesisInputStream, final TimeUnit unit, final long time,
                                                     final int kinesisRecordsPerRequest, final int streamChunkingSize,
                                                     final ForkJoinPool forkJoinPool) {
        // i know we could lose persision, but in this case, its ok
        int numberOfMinutes = TimeUnit.HOURS == unit ? Long.valueOf(unit.toMinutes(time)).intValue() :
                Long.valueOf(time).intValue();
        final CountdownTimer countdownTimer = new CountdownTimer();
        countdownTimer.countDown(numberOfMinutes);

        while (true) {
            dispatchEventsToKinesisAsync(amazonKinesisClient, kinesisInputStream, 1000, kinesisRecordsPerRequest,
                    streamChunkingSize, forkJoinPool);
            if (countdownTimer.isFinished()) {
                break;
            }
        }
        // we've reached the maximum time allocated
        LOGGER.info("finished pushing events for {} {}", time, unit);
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
                        amazonKinesisClient.putRecordsAsync(putRecordsRequest, new AsyncHandler<PutRecordsRequest,
                                PutRecordsResult>() {
                            @Override
                            public void onError(final Exception exception) {
                                final long error = SYSTEM_ERROR.addAndGet(putRecordsRequest.getRecords().size());
                                LOGGER.error("unable to dispatch: {} events (system error)", error, exception);
                            }

                            @Override
                            public void onSuccess(final PutRecordsRequest request, final PutRecordsResult putRecordsResult) {
                                if (putRecordsResult.getFailedRecordCount() == 0) {
                                    SUCCESS.getAndAdd(request.getRecords().size());
                                }

                                if (putRecordsResult.getFailedRecordCount() > 0) {
                                    final Integer failedRecordCount = putRecordsResult.getFailedRecordCount();
                                    // add the records that succeeded
                                    SUCCESS.getAndAdd(request.getRecords().size() - failedRecordCount);
                                    // add failures
                                    KINESIS_ERROR.getAndAdd(failedRecordCount);
                                    LOGGER.error("unable to dispatch: {} events (kinesis error)", failedRecordCount);
                                }
                            }
                        });
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
                                LOGGER.debug("known failure: >{}<", failed.get(i).toString());
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
    }

    private static List<PutRecordsRequestEntry> generatePutRecordsRequestEntries(final int numberOfEvents) {
        return generateEvents(numberOfEvents, "KinesisClientGenerated").parallelStream().map(event -> {
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
        EventRunParms runParms = null;
        AmazonKinesisAsyncClient amazonKinesisClient = null;
        ForkJoinPool forkJoinPool = null;

        //noinspection finally
        try {
            if (args == null) {
                throw new IllegalArgumentException(
                        "ensure all the following are set {kinesisEndpoint, kinesisInputStream, numberOfEvents, " +
                                "kinesisRecordsPerRequest, streamChunkSize, levelOfParallelism} or " +
                                "{kinesisEndpoint, kinesisInputStream, timeUnit, time, kinesisRecordsPerRequest, " +
                                "streamChunkSize, levelOfParallelism}");
            }

            // create the runtime params for summary
            runParms = createEventRunParms(args);

            final String kinesisEndpoint = args[0];
            final String kinesisStream = args[1];

            if (args.length == 6) {
                final long numberOfEvents = Long.valueOf(args[2]);
                final int kinesisRecordsPerRequest = Integer.valueOf(args[3]);
                final int streamChunkSize = Integer.valueOf(args[4]);
                final int parallelism = Integer.valueOf(args[5]);
                amazonKinesisClient = getAmazonAsyncKinesisClient(kinesisEndpoint, parallelism);
                forkJoinPool =
                        new ForkJoinPool(parallelism, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
                dispatchEventsByNumber(amazonKinesisClient, kinesisStream, numberOfEvents, kinesisRecordsPerRequest,
                        streamChunkSize, forkJoinPool);
            } else if (args.length == 7) {
                final TimeUnit unit = TimeUnit.valueOf(args[2]);
                final long time = Long.valueOf(args[3]);
                final int kinesisRecordsPerRequest = Integer.valueOf(args[4]);
                final int streamChunkSize = Integer.valueOf(args[5]);
                final int parallelism = Integer.valueOf(args[6]);
                amazonKinesisClient = getAmazonAsyncKinesisClient(kinesisEndpoint, parallelism);
                forkJoinPool =
                        new ForkJoinPool(parallelism, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
                dispatchEventsByTime(amazonKinesisClient, kinesisStream, unit, time, kinesisRecordsPerRequest,
                        streamChunkSize, forkJoinPool);
            } else {
                throw new IllegalArgumentException(String.format("invalid argument list %s", Arrays.toString(args)));
            }
        } catch (final Throwable any) {
            LOGGER.error("unexpected error running kinesis client", any);
        } finally {
            if (runParms != null) {
                try {
                    summarizeEventResults(runParms);
                } catch (final Throwable throwable) {
                    LOGGER.error("Unexpected error collecting summaries", throwable);
                }
            }
            if (forkJoinPool != null) {
                try {
                    // shutdown and wait for jobs to be finished
                    forkJoinPool.shutdown();
                    forkJoinPool.awaitTermination(5, TimeUnit.MINUTES);
                } catch (final Throwable throwable) {
                    LOGGER.error("unexpected error shutting down forkJoinPook", throwable);
                }
            }
            // shutdown client
            if (amazonKinesisClient != null) {
                try {
                    final ExecutorService executorService = amazonKinesisClient.getExecutorService();
                    executorService.shutdown();
                    executorService.awaitTermination(5, TimeUnit.MINUTES);
                } catch (final Throwable throwable) {
                    LOGGER.error("unexpected error shutting down amazon kinesis client", throwable);
                }
            }
            // kill everything
            System.exit(0);
        }
    }

    private static EventRunParms createEventRunParms(final String[] args) {
        //todo: clean up
        final EventRunParms runParms = new EventRunParms(args);
        runParms.setKinesisInputStream(args[1]);
        // set duration if set
        if (args.length == 7) {
            final long time = Long.valueOf(args[3]);
            runParms.setDuration(time);
        }
        return runParms;
    }

    private static void dispatchEventsByNumber(final AmazonKinesisAsyncClient amazonKinesisClient,
                                               final String kinesisInputStream, final long numberOfEvents,
                                               final int kinesisRecordsPerRequest, final int streamChunkSize,
                                               final ForkJoinPool forkJoinPool) {
        //generate events and push to Kinesis
        dispatchEventsToKinesisAsync(amazonKinesisClient, kinesisInputStream, numberOfEvents, kinesisRecordsPerRequest,
                streamChunkSize, forkJoinPool);
    }

    private static void dispatchEventsByTime(final AmazonKinesisAsyncClient amazonKinesisClient,
                                             final String kinesisInputStream, final TimeUnit unit, final Long time,
                                             final int kinesisRecordsPerRequest, final int streamChunkSize,
                                             final ForkJoinPool forkJoinPool) {
        //generate events and push to Kinesis
        dispatchEventsToKinesisAsync(amazonKinesisClient, kinesisInputStream, unit, time,
                kinesisRecordsPerRequest, streamChunkSize, forkJoinPool);
    }

    /**
     * push summary results to dynamodb
     */
    public static void summarizeEventResults(final EventRunParms eventRunParms) {
        eventRunParms.setRunEndTime(DateTime.now());
        final KinesisEventSummary summary = new KinesisEventSummary(eventRunParms);
        summary.setTotalEventsAttempted(TOTAL_EVENTS.get());
        summary.setTotalEventsSucceeded(SUCCESS.get());
        summary.setTotalEventsFailed(SYSTEM_ERROR.get() + KINESIS_ERROR.get());
        summary.setTotalEventsFailedKinesisErrors(KINESIS_ERROR.get());
        summary.setTotalEventsFailedSystemErrors(SYSTEM_ERROR.get());

        summary.logSummaryStats(); // output to console
        final KinesisEventSummaryPersister reporter =
                new KinesisEventSummaryPersister(eventRunParms.getDynamoDBEndPoint(), null);
        reporter.persistSummary(summary);
    }
}
