package com.dematic.labs.toolkit.simulators.grainger;

public final class OpcTagReadingExecutor {

    private static double signalsPerSecond(final int signalsPerMinutes) {
        return (double) Math.round((signalsPerMinutes / 60d) * 100) / 100;
    }

    public static void main(String[] args) {
        if (args == null || args.length < 7) {
            // 100 200 30 2 3 https://kinesis.us-west-2.amazonaws.com  node_test unittest jobId
            throw new IllegalArgumentException("OpcTagReadingExecutor: Please ensure the following are set: opcTagRangeMin, " +
                    "opcTagRangeMax, maxSignalsPerMinutePerNode, durationInMinutes, kafkaServerBootstrap,"
                    + "kafkaTopics, and generatorId");
        }
        final int opcTagRangeMin = Integer.valueOf(args[0]);
        final int opcTagRangeMax = Integer.valueOf(args[1]);
        final int maxSignalsPerMinutePerNode = Integer.valueOf(args[2]);
        final long durationInMinutes = Long.valueOf(args[3]);
        final String kafkaServerBootstrap = args[4];
        final String kafkaTopics = args[5];
        final String generatorId = args[6];

        try {
        } finally {
            Runtime.getRuntime().halt(0);
        }
    }
}
