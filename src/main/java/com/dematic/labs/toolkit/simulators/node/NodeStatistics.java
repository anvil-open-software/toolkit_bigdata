package com.dematic.labs.toolkit.simulators.node;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

final class NodeStatistics {
    private final Map<String, AtomicInteger> totalSuccessEventCounts;
    private final Map<String, AtomicInteger> totalErrorEventCounts;
    private final Map<String, AtomicInteger> completedJobCounts;
    private final Map<String, AtomicInteger> eventCycleTimeStartErrorCounts;
    private final Map<String, AtomicInteger> eventCycleTimeEndErrorCounts;

    NodeStatistics() {
        totalSuccessEventCounts = Maps.newConcurrentMap();
        totalErrorEventCounts = Maps.newConcurrentMap();
        completedJobCounts = Maps.newConcurrentMap();
        eventCycleTimeStartErrorCounts = Maps.newConcurrentMap();
        eventCycleTimeEndErrorCounts = Maps.newConcurrentMap();
    }

    void incrementEventSuccessCountByNode(final String nodeId) {
        if (totalSuccessEventCounts.containsKey(nodeId)) {
            totalSuccessEventCounts.get(nodeId).getAndIncrement();
        } else {
            totalSuccessEventCounts.put(nodeId, new AtomicInteger(1));
        }
    }

    void incrementEventErrorCountByNode(final String nodeId) {
        if (totalErrorEventCounts.containsKey(nodeId)) {
            totalErrorEventCounts.get(nodeId).getAndIncrement();
        } else {
            totalErrorEventCounts.put(nodeId, new AtomicInteger(1));
        }
    }

    void incrementCompletedJobCounts(final String nodeId) {
        if (completedJobCounts.containsKey(nodeId)) {
            completedJobCounts.get(nodeId).getAndIncrement();
        } else {
            completedJobCounts.put(nodeId, new AtomicInteger(1));
        }
    }

    void incrementEventCycleTimeStartErrorCounts(final String nodeId) {
        if (eventCycleTimeStartErrorCounts.containsKey(nodeId)) {
            eventCycleTimeStartErrorCounts.get(nodeId).getAndIncrement();
        } else {
            eventCycleTimeStartErrorCounts.put(nodeId, new AtomicInteger(1));
        }
    }

    void incrementEventCycleTimeEndErrorCounts(final String nodeId) {
        if (eventCycleTimeEndErrorCounts.containsKey(nodeId)) {
            eventCycleTimeEndErrorCounts.get(nodeId).getAndIncrement();
        } else {
            eventCycleTimeEndErrorCounts.put(nodeId, new AtomicInteger(1));
        }
    }

    int getTotalSuccessEventCounts() {
        final int[] count = {0};
        totalSuccessEventCounts.values().stream().forEach(value -> count[0] = count[0] + value.get());
        return count[0];
    }

    int getTotalSuccessEventCountsByNodeId(final String nodeId) {
        final AtomicInteger count = totalSuccessEventCounts.get(nodeId);
        return count != null ? count.get() : 0;
    }

    int getTotalErrorEventCounts() {
        final int[] count = {0};
        totalErrorEventCounts.values().stream().forEach(value -> count[0] = count[0] + value.get());
        return count[0];
    }

    int getTotalErrorEventCountsByNodeId(final String nodeId) {
        final AtomicInteger count = totalErrorEventCounts.get(nodeId);
        return count != null ? count.get() : 0;
    }

    int getCompletedJobCounts() {
        final int[] count = {0};
        completedJobCounts.values().stream().forEach(value -> count[0] = count[0] + value.get());
        return count[0];
    }

    int getCompletedJobCountsByNodeId(final String nodeId) {
        final AtomicInteger count = completedJobCounts.get(nodeId);
        return count != null ? count.get() : 0;
    }

    int getEventCycleTimeStartErrorCounts() {
        final int[] count = {0};
        eventCycleTimeStartErrorCounts.values().stream().forEach(value -> count[0] = count[0] + value.get());
        return count[0];
    }

    int getEventCycleTimeStartErrorCountsByNodeId(final String nodeId) {
        final AtomicInteger count = eventCycleTimeStartErrorCounts.get(nodeId);
        return count != null ? count.get() : 0;
    }

    int getEventCycleTimeEndErrorCounts() {
        final int[] count = {0};
        eventCycleTimeEndErrorCounts.values().stream().forEach(value -> count[0] = count[0] + value.get());
        return count[0];
    }

    int getEventCycleTimeEndErrorCountsByNodeId(final String nodeId) {
        final AtomicInteger count = eventCycleTimeEndErrorCounts.get(nodeId);
        return count != null ? count.get() : 0;
    }
}
