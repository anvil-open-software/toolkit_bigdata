package com.dematic.labs.toolkit.simulators.node;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.dematic.labs.toolkit.CountdownTimer;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventSequenceNumber;
import com.dematic.labs.toolkit.communication.EventType;
import com.dematic.labs.toolkit.simulators.statistics;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.dematic.labs.toolkit.aws.Connections.getAmazonKinesisClient;
import static com.dematic.labs.toolkit.aws.kinesis.KinesisClient.dispatchSingleEventToKinesisWithRetries;
import static com.dematic.labs.toolkit.communication.EventUtils.now;

public final class NodeExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeExecutor.class);
    private static final int RETRY = 3;

    private final int nodeRangeSize;
    private final Stream<String> nodeRangeIds;
    private final int maxEventsPerMinutePerNode;
    private final int avgInterArrivalTime;
    private final String generatorId;
    private final String groupBy;
    private final com.dematic.labs.toolkit.simulators.statistics statistics;

    private NodeExecutor(final int nodeRangeMin, final int nodeRangeMax, final int maxEventsPerMinutePerNode,
                         final int avgInterArrivalTime, final String generatorId, final String groupBy) {
        nodeRangeSize = nodeRangeMax - nodeRangeMin;
        nodeRangeIds = IntStream.range(nodeRangeMin, nodeRangeMax).mapToObj(i -> generatorId + "-" + i);
        this.maxEventsPerMinutePerNode = maxEventsPerMinutePerNode;
        this.avgInterArrivalTime = avgInterArrivalTime;
        this.generatorId = generatorId;
        this.groupBy = groupBy;
        statistics = new statistics();
        LOGGER.info("NodeExecutor: created with a nodeRangeSize {} between {} and {} with maxEventsPerMinutePerNode {} "
                        + "and an averger inter-arrival time of {} and generatorId {}", nodeRangeSize, nodeRangeMin,
                nodeRangeMax, maxEventsPerMinutePerNode, avgInterArrivalTime, generatorId);
    }

    public void execute(final Long durationInMinutes, final String kinesisEndpoint, final String kinesisStreamName) {
        final CountDownLatch latch = new CountDownLatch(nodeRangeSize);
        final ForkJoinPool forkJoinPool =
                new ForkJoinPool(nodeRangeSize, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
        try {
            nodeRangeIds.forEach(nodeId -> forkJoinPool.submit(() ->
                    dispatchPerNode(kinesisEndpoint, kinesisStreamName, nodeId, durationInMinutes, latch)));
            // wait 5 minutes longer then duration
            latch.await(durationInMinutes + 5, TimeUnit.MINUTES);
        } catch (final Throwable any) {
            LOGGER.error("NodeExecutor: Unhandled Error: stopping execution", any);
        } finally {
            try {
                if ("jobId".equalsIgnoreCase(groupBy)) {
                    LOGGER.info("NodeExecutor: Total Success Events: {}", statistics.getTotalSuccessCounts());
                    LOGGER.info("NodeExecutor: Total CT Jobs: {}", statistics.getCompletedCounts());
                    LOGGER.info("NodeExecutor: Total Errors: {}", statistics.getTotalErrorCounts());
                    LOGGER.info("NodeExecutor: Total Start Event Errors: {}", statistics.getCycleTimeStartErrorCounts());
                    LOGGER.info("NodeExecutor: Total End Event Errors: {}", statistics.getCycleTimeEndErrorCounts());
                } else {
                    LOGGER.info("NodeExecutor: Total Success Events: {}", statistics.getTotalSuccessCounts());
                    LOGGER.info("NodeExecutor: Total Errors: {}", statistics.getTotalErrorCounts());
                }
            } catch (final Throwable ignore) {
            }
            try {
                forkJoinPool.shutdownNow();
            } catch (final Throwable ignore) {
            }
        }
    }

    private void dispatchPerNode(final String kinesisEndpoint, final String kinesisStreamName, final String nodeId,
                                 final Long durationInMinutes, final CountDownLatch latch) {
        LOGGER.debug("NodeExecutor: Dispatching events for {}", nodeId);

        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.withMaxConnections(150).withMaxErrorRetry(RETRY);
        final AmazonKinesisClient amazonKinesisClient = getAmazonKinesisClient(kinesisEndpoint, clientConfiguration);

        try {
            // generate events for the specific amount of time in minutes for a specific node
            final CountdownTimer countdownTimer = new CountdownTimer();
            countdownTimer.countDown((int) TimeUnit.MINUTES.toMinutes(durationInMinutes));

            final RateLimiter rateLimiter = RateLimiter.create(eventsPerSecond(maxEventsPerMinutePerNode));
            final Random randomNumberGenerator = new Random();

            while (true) {
                if (Strings.isNullOrEmpty(groupBy)) {
                    // dispatchSingleEvent until duration ends at a rate specified by max events
                    dispatchSingleEvent(amazonKinesisClient, kinesisStreamName, nodeId, rateLimiter, randomNumberGenerator);
                } else if ("jobId".equalsIgnoreCase(groupBy)) {
                    // dispatchSingleEvents in pairs groupBy 'jobId' until duration ends at a rate specified by max events
                    dispatchSingleEventByJobId(amazonKinesisClient, kinesisStreamName, nodeId, rateLimiter,
                            randomNumberGenerator);
                } else {
                    throw new IllegalStateException(String.format("NodeExecutor: Unexpected parameter >%s<", groupBy));
                }

                if (countdownTimer.isFinished()) {
                    if ("jobId".equalsIgnoreCase(groupBy)) {
                        LOGGER.debug("NodeExecutor: Completed dispatching events for {} ", nodeId);
                        LOGGER.debug("NodeExecutor: Success Events: {}", statistics.getTotalSuccessCountsById(nodeId));
                        LOGGER.debug("NodeExecutor: CT Jobs: {}", statistics.getCompletedJobCountsById(nodeId));
                        LOGGER.debug("NodeExecutor: Errors: {}", statistics.getTotalErrorCountsById(nodeId));
                        LOGGER.debug("NodeExecutor: Start Event Errors: {}", statistics.getCycleTimeStartErrorCountsById(nodeId));
                        LOGGER.debug("NodeExecutor: End Event Errors: {}", statistics.getCycleTimeEndErrorCountsById(nodeId));
                    } else {
                        LOGGER.debug("NodeExecutor: Completed dispatching events for {} ", nodeId);
                        LOGGER.debug("\tNodeExecutor: {} : Success Events {}", nodeId, statistics.getTotalSuccessCountsById(nodeId));
                        LOGGER.debug("\tNodeExecutor: {} : Error {}", nodeId, statistics.getTotalErrorCountsById(nodeId));
                    }
                    break;
                }
            }
        } finally {
            latch.countDown();
        }
    }

    private void dispatchSingleEvent(final AmazonKinesisClient kinesisEventClient, final String kinesisStreamName,
                                     final String nodeId, final RateLimiter rateLimiter,
                                     final Random randomNumberGenerator) {
        rateLimiter.acquire();
        // event time is based on the avgInterArrivalTime * bounded random int, 6 is the upper bounds, inclusive

        final DateTime now = now().plusSeconds(randomNumberGenerator.nextInt(7) * avgInterArrivalTime);

        // if we fail we will try to just dispatchSingleEvent another event, because of how kinesis works, a failure doesn't
        // mean the event didn't go through, we could have had a network error and we are unable to tell if the
        // event made it or not, we are just going to dispatchSingleEvent another event
        final boolean success = dispatchSingleEventToKinesisWithRetries(kinesisEventClient, kinesisStreamName,
                new Event(UUID.randomUUID(), EventSequenceNumber.next(), nodeId, UUID.randomUUID(), EventType.UNKNOWN,
                        now, generatorId, null), RETRY);
        // increment counts
        if (success) {
            statistics.incrementSuccessCountById(nodeId);
        } else {
            statistics.incrementErrorCountById(nodeId);
        }
    }

    private void dispatchSingleEventByJobId(final AmazonKinesisClient kinesisEventClient,
                                            final String kinesisStreamName, final String nodeId,
                                            final RateLimiter rateLimiter, final Random randomNumberGenerator) {
        rateLimiter.acquire();
        // event time is based on the avgInterArrivalTime * bounded random int, 6 is the upper bounds, inclusive, and
        // end needs to come after start
        final DateTime start = now().plusSeconds(randomNumberGenerator.nextInt(7) * avgInterArrivalTime);
        final DateTime end = start.plusSeconds(randomNumberGenerator.nextInt(7) * avgInterArrivalTime);

        // if we fail we will try to just dispatchSingleEvent another event, because of how kinesis works, a failure doesn't
        // mean the event didn't go through, we could have had a network error and we are unable to tell if the
        // event made it or not, we are just going to dispatchSingleEvent another event
        final UUID jobId = UUID.randomUUID();

        // dispatch a start based on jobId
        final boolean startSuccess = dispatchSingleEventToKinesisWithRetries(kinesisEventClient, kinesisStreamName,
                new Event(UUID.randomUUID(), EventSequenceNumber.next(), nodeId, jobId, EventType.START, start,
                        generatorId, null), RETRY);
        // dispatch a end event based on jobId
        final boolean endSuccess = dispatchSingleEventToKinesisWithRetries(kinesisEventClient, kinesisStreamName,
                new Event(UUID.randomUUID(), EventSequenceNumber.next(), nodeId, jobId, EventType.END, end, generatorId,
                        null), RETRY);
        // increment counts
        if(startSuccess && endSuccess) {
            // 2 events per job
            statistics.incrementSuccessCountById(nodeId);
            statistics.incrementSuccessCountById(nodeId);
            statistics.incrementCompletedCounts(nodeId);
        }
        if (!startSuccess) {
            statistics.incrementErrorCountById(nodeId);
            statistics.incrementCycleTimeStartErrorCounts(nodeId);
        }
        if(!endSuccess) {
            statistics.incrementErrorCountById(nodeId);
            statistics.incrementCycleTimeEndErrorCounts(nodeId);
        }
    }

    private static double eventsPerSecond(final int eventsPerMinutes) {
        return (double) Math.round((eventsPerMinutes / 60d) * 100) / 100;
    }

    public static void main(String[] args) {
        if (args == null || args.length < 8) {
            // 100 200 30 2 3 https://kinesis.us-west-2.amazonaws.com  node_test unittest jobId
            throw new IllegalArgumentException("NodeExecutor: Please ensure the following are set: nodeRangeMin, " +
                    "nodeRangeMax, maxEventsPerMinutePerNode, avgArrivalTime, duriationInMinutes, kinesisEndpoint,"
                    + "kinesisStreamName, generatorId, and optional groupBy");
        }
        final int nodeRangeMin = Integer.valueOf(args[0]);
        final int nodeRangeMax = Integer.valueOf(args[1]);
        final int maxEventsPerMinutePerNode = Integer.valueOf(args[2]);
        final int avgInterArrivalTime = Integer.valueOf(args[3]);
        final long duriationInMinutes = Long.valueOf(args[4]);
        final String kinesisEndpoint = args[5];
        final String kinesisStreamName = args[6];
        final String generatorId = args[7];
        // for now, we will just groupBy jobId
        final String groupBy = args.length == 9 ? args[8] : null;

        try {
            final NodeExecutor nodeExecutor = new NodeExecutor(nodeRangeMin, nodeRangeMax, maxEventsPerMinutePerNode,
                    avgInterArrivalTime, generatorId, groupBy);
            nodeExecutor.execute(duriationInMinutes, kinesisEndpoint, kinesisStreamName);
        } finally {
            Runtime.getRuntime().halt(0);
        }
    }
}
