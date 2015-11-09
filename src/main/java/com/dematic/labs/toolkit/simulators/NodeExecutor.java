package com.dematic.labs.toolkit.simulators;

import com.dematic.labs.toolkit.CountdownTimer;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventSequenceNumber;
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

import static com.dematic.labs.toolkit.aws.kinesis.KinesisEventClient.dispatchEventsToKinesis;
import static com.dematic.labs.toolkit.communication.EventUtils.*;
import static java.util.Collections.singletonList;

public final class NodeExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeExecutor.class);

    private final int nodeRangeSize;
    private final Stream<String> nodeRangeIds;
    private final int maxEventsPerMinutePerNode;
    private final int avgInterArrivalTime;
    private final String generatorId;

    public NodeExecutor(final int nodeRangeMin, final int nodeRangeMax, final int maxEventsPerMinutePerNode,
                        final int avgInterArrivalTime, final String generatorId) {
        nodeRangeSize = nodeRangeMax - nodeRangeMin;
        nodeRangeIds = IntStream.range(nodeRangeMin, nodeRangeMax).mapToObj(i -> "Node-" + i);
        this.maxEventsPerMinutePerNode = maxEventsPerMinutePerNode;
        this.avgInterArrivalTime = avgInterArrivalTime;
        this.generatorId = generatorId;
        LOGGER.info("NodeExecutor: created with a nodeRangeSize {} between {} and {} with maxEventsPerMinutePerNode {} "
                        + "and an averger inter-arrival time of {} and generatorId {}", nodeRangeSize, nodeRangeMin,
                nodeRangeMax, maxEventsPerMinutePerNode, avgInterArrivalTime, generatorId);
    }

    public void execute(final Long durationInMinutes, final String kinesisEndpoint, final String kinesisStreamName) {
        final CountDownLatch latch = new CountDownLatch(nodeRangeSize);
        final ForkJoinPool forkJoinPool =
                new ForkJoinPool(100, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
        try {
            nodeRangeIds.forEach(nodeId -> forkJoinPool.submit(() -> {
                dispatchPerNode(kinesisEndpoint, kinesisStreamName, nodeId, durationInMinutes, latch);
            }));
            // wait 5 minutes longer then duration
            latch.await(durationInMinutes + 5, TimeUnit.MINUTES);
        } catch (final Throwable any) {
            LOGGER.error("NodeExecutor: Unhandled Error: stopping execution", any);
        } finally {
            System.out.println("finally");
        }
    }

    private void dispatchPerNode(final String kinesisEndpoint, final String kinesisStreamName, final String nodeId,
                                 final Long durationInMinutes, final CountDownLatch latch) {
        LOGGER.info("NodeExecutor: Dispatching events for node {}", nodeId);
        try {
            // generate events for the specific amount of time in minutes for a specific node
            final CountdownTimer countdownTimer = new CountdownTimer();
            countdownTimer.countDown((int) TimeUnit.MINUTES.toMinutes(durationInMinutes));
            final RateLimiter rateLimiter = RateLimiter.create(eventsPerSecond(maxEventsPerMinutePerNode));
            final Random randomNumberGenerator = new Random();
            // dispatch until duration ends at a rate specified by max events
            while (true) {
                dispatch(kinesisEndpoint, kinesisStreamName, nodeId, rateLimiter, randomNumberGenerator);
                if (countdownTimer.isFinished()) {
                    LOGGER.info("NodeExecutor: Completed dispatching events for node {}", nodeId);
                    break;
                }
            }
        } finally {
            latch.countDown();
        }
    }

    private void dispatch(final String kinesisEndpoint, final String kinesisStreamName, final String nodeId,
                          final RateLimiter rateLimiter, final Random randomNumberGenerator) {
        rateLimiter.acquire();
        // event time is based on the avgInterArrivalTime * bounded random int, 6 is the upper bounds, inclusive
        final DateTime now = now().plusSeconds(randomNumberGenerator.nextInt(7) * avgInterArrivalTime);
        // todo: come back to batching and errors
        dispatchEventsToKinesis(kinesisEndpoint, kinesisStreamName, singletonList(new Event(UUID.randomUUID(),
                EventSequenceNumber.next(), nodeId, null, now, generatorId, null)));
    }

    private static double eventsPerSecond(final int eventsPerMinutes) {
        return (double) Math.round((eventsPerMinutes / 60d) * 100) / 100;
    }

    public static void main(String[] args) {
        if (args == null || args.length != 8) {
            throw new IllegalArgumentException();
        }
        final int nodeRangeMin = Integer.valueOf(args[0]);
        final int nodeRangeMax = Integer.valueOf(args[1]);
        final int maxEventsPerMinutePerNode = Integer.valueOf(args[2]);
        final int avgInterArrivalTime = Integer.valueOf(args[3]);
        final long duriationInMinutes = Long.valueOf(args[4]);
        final String kinesisEndpoint = args[5];
        final String kinesisStreamName = args[6];
        final String generatorId = args[7];

        final NodeExecutor nodeExecutor = new NodeExecutor(nodeRangeMin, nodeRangeMax, maxEventsPerMinutePerNode,
                avgInterArrivalTime, generatorId);
        nodeExecutor.execute(duriationInMinutes, kinesisEndpoint, kinesisStreamName);
    }
}
