/*
 *  Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.toolkit_bigdata.simulators;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("unused")
public final class Statistics {
    private final Map<String, AtomicInteger> totalSuccessCounts;
    private final Map<String, AtomicInteger> totalErrorCounts;
    private final Map<String, AtomicInteger> completedCounts;
    private final Map<String, AtomicInteger> cycleTimeStartErrorCounts;
    private final Map<String, AtomicInteger> cycleTimeEndErrorCounts;

    public Statistics() {
        totalSuccessCounts = Maps.newConcurrentMap();
        totalErrorCounts = Maps.newConcurrentMap();
        completedCounts = Maps.newConcurrentMap();
        cycleTimeStartErrorCounts = Maps.newConcurrentMap();
        cycleTimeEndErrorCounts = Maps.newConcurrentMap();
    }

    public void incrementSuccessCountById(final String id) {
        if (totalSuccessCounts.containsKey(id)) {
            totalSuccessCounts.get(id).getAndIncrement();
        } else {
            totalSuccessCounts.put(id, new AtomicInteger(1));
        }
    }

    public void incrementErrorCountById(final String id) {
        if (totalErrorCounts.containsKey(id)) {
            totalErrorCounts.get(id).getAndIncrement();
        } else {
            totalErrorCounts.put(id, new AtomicInteger(1));
        }
    }

    public void incrementCompletedCounts(final String id) {
        if (completedCounts.containsKey(id)) {
            completedCounts.get(id).getAndIncrement();
        } else {
            completedCounts.put(id, new AtomicInteger(1));
        }
    }

    public void incrementCycleTimeStartErrorCounts(final String id) {
        if (cycleTimeStartErrorCounts.containsKey(id)) {
            cycleTimeStartErrorCounts.get(id).getAndIncrement();
        } else {
            cycleTimeStartErrorCounts.put(id, new AtomicInteger(1));
        }
    }

    public void incrementCycleTimeEndErrorCounts(final String id) {
        if (cycleTimeEndErrorCounts.containsKey(id)) {
            cycleTimeEndErrorCounts.get(id).getAndIncrement();
        } else {
            cycleTimeEndErrorCounts.put(id, new AtomicInteger(1));
        }
    }

    public int getTotalSuccessCounts() {
        final int[] count = {0};
        totalSuccessCounts.values().forEach(value -> count[0] = count[0] + value.get());
        return count[0];
    }

    public int getTotalSuccessCountsById(final String id) {
        final AtomicInteger count = totalSuccessCounts.get(id);
        return count != null ? count.get() : 0;
    }

    public int getTotalErrorCounts() {
        final int[] count = {0};
        totalErrorCounts.values().forEach(value -> count[0] = count[0] + value.get());
        return count[0];
    }

    public int getTotalErrorCountsById(final String id) {
        final AtomicInteger count = totalErrorCounts.get(id);
        return count != null ? count.get() : 0;
    }

    public int getCompletedCounts() {
        final int[] count = {0};
        completedCounts.values().forEach(value -> count[0] = count[0] + value.get());
        return count[0];
    }

    public int getCompletedJobCountsById(final String id) {
        final AtomicInteger count = completedCounts.get(id);
        return count != null ? count.get() : 0;
    }

    public int getCycleTimeStartErrorCounts() {
        final int[] count = {0};
        cycleTimeStartErrorCounts.values().forEach(value -> count[0] = count[0] + value.get());
        return count[0];
    }

    public int getCycleTimeStartErrorCountsById(final String id) {
        final AtomicInteger count = cycleTimeStartErrorCounts.get(id);
        return count != null ? count.get() : 0;
    }

    public int getCycleTimeEndErrorCounts() {
        final int[] count = {0};
        cycleTimeEndErrorCounts.values().forEach(value -> count[0] = count[0] + value.get());
        return count[0];
    }

    public int getCycleTimeEndErrorCountsById(final String id) {
        final AtomicInteger count = cycleTimeEndErrorCounts.get(id);
        return count != null ? count.get() : 0;
    }
}
