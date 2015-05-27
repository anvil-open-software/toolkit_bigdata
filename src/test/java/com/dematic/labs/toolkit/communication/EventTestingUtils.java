package com.dematic.labs.toolkit.communication;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.LongStream;

public final class EventTestingUtils {
    private EventTestingUtils() {
    }

    /**
     * Generate analytic system events.
     *
     * @param numberOfEvents -- # of events to generate
     * @param nodeSize       -- amount of nodes
     * @param orderSize      -- amout of orders
     * @return List<Event>   -- list of generated events
     */
    public static List<Event> generateEvents(final long numberOfEvents, final int nodeSize, final int orderSize) {
        final Random randomGenerator = new Random();
        // startInclusive the (inclusive) initial value, endExclusive the exclusive upper bound
        return LongStream.range(1, numberOfEvents + 1)
                .parallel()
                .mapToObj(value -> new Event(UUID.randomUUID(), randomGenerator.nextInt(nodeSize) + 1,
                        randomGenerator.nextInt(orderSize) + 1, DateTime.now(),
                        Math.abs((int) Math.round(randomGenerator.nextGaussian() * orderSize + nodeSize))))
                        //supplier, accumulator, combiner
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }
}
