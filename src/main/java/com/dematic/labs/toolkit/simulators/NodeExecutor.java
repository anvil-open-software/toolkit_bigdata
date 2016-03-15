package com.dematic.labs.toolkit.simulators;

import com.dematic.labs.toolkit.CountdownTimer;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventSequenceNumber;
import com.dematic.labs.toolkit.communication.EventType;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.dematic.labs.toolkit.aws.kinesis.KinesisEventClient.dispatchEventsToKinesisWithRetries;
import static com.dematic.labs.toolkit.communication.EventUtils.*;
import static java.util.Collections.singletonList;

public final class NodeExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeExecutor.class);

    private final int nodeRangeSize;
    private final Stream<String> nodeRangeIds;
    private final int maxEventsPerMinutePerNode;
    private final int avgInterArrivalTime;
    private final String generatorId;
    private final String groupBy;
    private final Map<String, AtomicInteger> statistics;

    public NodeExecutor(final int nodeRangeMin, final int nodeRangeMax, final int maxEventsPerMinutePerNode,
                        final int avgInterArrivalTime, final String generatorId, final String groupBy) {
        nodeRangeSize = nodeRangeMax - nodeRangeMin;
        nodeRangeIds = IntStream.range(nodeRangeMin, nodeRangeMax).mapToObj(i -> generatorId + "-" + i);
        this.maxEventsPerMinutePerNode = maxEventsPerMinutePerNode;
        this.avgInterArrivalTime = avgInterArrivalTime;
        this.generatorId = generatorId;
        this.groupBy = groupBy;
        statistics = Maps.newConcurrentMap();
        LOGGER.info("NodeExecutor: created with a nodeRangeSize {} between {} and {} with maxEventsPerMinutePerNode {} "
                        + "and an averger inter-arrival time of {} and generatorId {}", nodeRangeSize, nodeRangeMin,
                nodeRangeMax, maxEventsPerMinutePerNode, avgInterArrivalTime, generatorId);
    }

    public void execute(final Long durationInMinutes, final String kinesisEndpoint, final String kinesisStreamName) {
        final CountDownLatch latch = new CountDownLatch(nodeRangeSize);
        final ForkJoinPool forkJoinPool =
                new ForkJoinPool(nodeRangeSize, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
        try {
            nodeRangeIds.forEach(nodeId -> forkJoinPool.submit(() -> {
                dispatchPerNode(kinesisEndpoint, kinesisStreamName, nodeId, durationInMinutes, latch);
            }));
            // wait 5 minutes longer then duration
            latch.await(durationInMinutes + 5, TimeUnit.MINUTES);
        } catch (final Throwable any) {
            LOGGER.error("NodeExecutor: Unhandled Error: stopping execution", any);
        } finally {
            try {
                forkJoinPool.shutdownNow();
            } catch (final Throwable ignore) {
            }
        }
    }

    private void dispatchPerNode(final String kinesisEndpoint, final String kinesisStreamName, final String nodeId,
                                 final Long durationInMinutes, final CountDownLatch latch) {
        LOGGER.info("NodeExecutor: Dispatching events for {}", nodeId);
        try {
            // generate events for the specific amount of time in minutes for a specific node
            final CountdownTimer countdownTimer = new CountdownTimer();
            countdownTimer.countDown((int) TimeUnit.MINUTES.toMinutes(durationInMinutes));

            final RateLimiter rateLimiter = RateLimiter.create(eventsPerSecond(maxEventsPerMinutePerNode));
            final Random randomNumberGenerator = new Random();

            while (true) {
                if (Strings.isNullOrEmpty(groupBy)) {
                    // dispatchSingleEvent until duration ends at a rate specified by max events
                    dispatchSingleEvent(kinesisEndpoint, kinesisStreamName, nodeId, rateLimiter, randomNumberGenerator);
                } else if ("jobId".equalsIgnoreCase(groupBy)) {
                    // dispatchSingleEvents in pairs groupBy 'jobId' until duration ends at a rate specified by max events
                    dispatchSingleEventByJobId(kinesisEndpoint, kinesisStreamName, nodeId, rateLimiter,
                            randomNumberGenerator);
                } else {
                    throw new IllegalStateException(String.format("NodeExecutor: Unexpected parameter >%s<", groupBy));
                }

                if (countdownTimer.isFinished()) {
                    LOGGER.info("NodeExecutor: Completed dispatching events for {} : dispatched {} events",
                            nodeId, statistics.get(nodeId).get());
                    break;
                }
            }
        } finally {
            latch.countDown();
        }
    }

    private void dispatchSingleEvent(final String kinesisEndpoint, final String kinesisStreamName, final String nodeId,
                                     final RateLimiter rateLimiter, final Random randomNumberGenerator) {
        rateLimiter.acquire();
        // event time is based on the avgInterArrivalTime * bounded random int, 6 is the upper bounds, inclusive

        final DateTime now = now().plusSeconds(randomNumberGenerator.nextInt(7) * avgInterArrivalTime);

        // if we fail we will try to just dispatchSingleEvent another event, because of how kinesis works, a failure doesn't
        // mean the event didn't go through, we could have had a network error and we are unable to tell if the
        // event made it or not, we are just going to dispatchSingleEvent another event
        dispatchEventsToKinesisWithRetries(kinesisEndpoint, kinesisStreamName,
                singletonList(new Event(UUID.randomUUID(), EventSequenceNumber.next(), nodeId, UUID.randomUUID(),
                        EventType.UNKNOWN, now, generatorId, null)), 3);
        // add the event count to statistics
        incrementEventCount(nodeId, statistics);
    }

    private void dispatchSingleEventByJobId(final String kinesisEndpoint, final String kinesisStreamName,
                                            final String nodeId, final RateLimiter rateLimiter,
                                            final Random randomNumberGenerator) {
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
        dispatchEventsToKinesisWithRetries(kinesisEndpoint, kinesisStreamName,
                singletonList(new Event(UUID.randomUUID(), EventSequenceNumber.next(), nodeId, jobId, EventType.START,
                        start, generatorId, null)), 3);
        // add the event count to statistics
        incrementEventCount(nodeId, statistics);

        // dispatch a end event based on jobId
        dispatchEventsToKinesisWithRetries(kinesisEndpoint, kinesisStreamName,
                singletonList(new Event(UUID.randomUUID(), EventSequenceNumber.next(), nodeId, jobId, EventType.END,
                        end, generatorId, null)), 3);
        // add the event count to statistics
        incrementEventCount(nodeId, statistics);
    }

    private static void incrementEventCount(final String nodeId, final Map<String, AtomicInteger> statistics) {
        if (statistics.containsKey(nodeId)) {
            statistics.get(nodeId).getAndIncrement();
        } else {
            statistics.put(nodeId, new AtomicInteger(1));
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
