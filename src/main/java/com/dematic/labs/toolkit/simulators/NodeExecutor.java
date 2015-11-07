package com.dematic.labs.toolkit.simulators;

import com.dematic.labs.toolkit.CountdownTimer;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.dematic.labs.toolkit.aws.kinesis.KinesisEventClient.dispatchEventsToKinesis;

public final class NodeExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeExecutor.class);

    final int nodeRangeSize;
    final Stream<String> nodeRangeIds;
    final int maxEventsPerMinutePerNode;

    public NodeExecutor(final int nodeRangeMin, final int nodeRangeMax, final int maxEventsPerMinutePerNode) {
        nodeRangeSize = nodeRangeMax - nodeRangeMin;
        nodeRangeIds = IntStream.range(nodeRangeMin, nodeRangeMax).mapToObj(i -> "Node-" + i);
        this.maxEventsPerMinutePerNode = maxEventsPerMinutePerNode;
        LOGGER.info("NodeExecutor: created with a nodeRangeSize {} between {} and {} with maxEventsPerMinutePerNode {}",
                nodeRangeSize, nodeRangeMin, nodeRangeMax, maxEventsPerMinutePerNode);
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
            any.printStackTrace();
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
            // dispatch until duration ends at a rate specified by max events
            while (true) {
                dispatch(kinesisEndpoint, kinesisStreamName, nodeId, rateLimiter);
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
                          final RateLimiter rateLimiter) {
        rateLimiter.acquire();
       //todo dispatchEventsToKinesis(kinesisEndpoint, kinesisStreamName, EventUtils.generateEvents());
    }

    private static double eventsPerSecond(final int eventsPerMinutes) {
        return (double) Math.round((eventsPerMinutes / 60d) * 100) / 100;
    }

    /**
     * Generates notifications with Node Id’s that are formed by the String: “Node-” followed by a
     * number between the minimum and maximum given in 1.a. converted to a String and padded with
     * zeros to the left so that nodes can be lexicographically ordered up to 100,000 nodes.
     * These are referred to as “Generator Nodes”
     * <p>
     * <p>
     * <p>
     * <p>
     * Generates Notifications at a rate equal or lower than the maximum given in 1.b for each node Id of its “Generator Nodes”
     * <p>
     * The timestamps of the generated Notifications are such that the inter-arrival time (see definition above) is distributed according to either a uniform distribution with bounds: 0 and 6 times MIAT or an exponential distribution with mean = MIAT
     * <p>
     * The notifications do not need to be generated in an order that is congruent with the order of its timestamps.
     * <p>
     * <p>
     * For this first implementation the rates will be of the order of tens or hundreds of notifications per second.
     */


    public static void main(String[] args) {
        final NodeExecutor nodeExecutor = new NodeExecutor(100, 200, 5);
        nodeExecutor.execute(5L, "", "");
    }
}
