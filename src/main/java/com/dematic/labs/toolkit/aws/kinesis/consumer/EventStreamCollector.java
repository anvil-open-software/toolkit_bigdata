package com.dematic.labs.toolkit.aws.kinesis.consumer;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.dematic.labs.toolkit.CountdownTimer;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.ConcurrentHashMultiset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.toolkit.aws.Connections.*;

public final class EventStreamCollector extends KinesisConnectorExecutorBase<Event, byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamCollector.class);

    private final ConcurrentHashMultiset<UUID> statistics;
    private final KinesisConnectorConfiguration config;

    public EventStreamCollector(final String appName, final String kinesisEndpoint, final String streamName,
                                final int shardCount) {
        final Properties properties = new Properties();
        properties.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, appName);
        properties.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, kinesisEndpoint);
        properties.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, streamName);
        properties.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM_SHARD_COUNT,
                String.valueOf(shardCount));
        config = new KinesisConnectorConfiguration(properties, getAWSCredentialsProvider());
        statistics = ConcurrentHashMultiset.create();
        initialize(config, new NullMetricsFactory());
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<Event, byte[]> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(new EventStreamsConnectorPipeline(statistics), config);
    }

    public ExecutorService startCollectingStatistics() {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(worker);
        return executorService;
    }

    public void stopCollectingStatistics() {
        worker.shutdown();
    }

    public int getEventCountAsOfNow() {
        return statistics.size();
    }

    public int getEventDuplicateCountAsOfNow() {
       // duplicates are the total size - minus the number of distinct elements
        return statistics.size() - statistics.entrySet().size();
    }

    public static void main(String[] args) {
        /*
        USE LEGACY SORT UNTIL BUG is FIXED in JVM

        java.lang.IllegalArgumentException: Comparison method violates its general contract!
	at java.util.TimSort.mergeHi(TimSort.java:895) ~[na:1.8.0_25]
	at java.util.TimSort.mergeAt(TimSort.java:512) ~[na:1.8.0_25]
	at java.util.TimSort.mergeCollapse(TimSort.java:437) ~[na:1.8.0_25]
	at java.util.TimSort.sort(TimSort.java:241) ~[na:1.8.0_25]
	at java.util.Arrays.sort(Arrays.java:1438) ~[na:1.8.0_25]
	at com.google.common.collect.Ordering.immutableSortedCopy(Ordering.java:846) ~[toolkit-1.0-SNAPSHOT-consumer.jar:na]
	at com.google.common.collect.Multisets.copyHighestCountFirst(Multisets.java:1095) ~[toolkit-1.0-SNAPSHOT-consumer.jar:na]
	at com.dematic.labs.toolkit.aws.kinesis.consumer.EventStreamCollector.getEventDuplicateCountAsOfNow(EventStreamCollector.java:64) ~[toolkit-1.0-SNAPSHOT-consumer.jar:na]
	at com.dematic.labs.toolkit.aws.kinesis.consumer.EventStreamCollector.main(EventStreamCollector.java:97) ~[toolkit-1.0-SNAPSHOT-consumer.jar:na]
         */
        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");

        EventStreamCollector collector = null;
        ExecutorService executorService = null;
        if (args == null || args.length != 4) {
            throw new IllegalArgumentException();
        }
        final String appName = args[0];
        final String kinesisEndpoint = args[1];
        final String kinesisStream = args[2];
        //noinspection finally
        try {
            final int numberOfMinutes = Integer.valueOf(args[3]);
            final int numberOfShards = getNumberOfShards(kinesisEndpoint, kinesisStream);
            collector = new EventStreamCollector(appName, kinesisEndpoint, kinesisStream, numberOfShards);
            executorService = collector.startCollectingStatistics();
            // collect for specific amount of time in minutes
            final CountdownTimer countdownTimer = new CountdownTimer();
            countdownTimer.countDown(numberOfMinutes);
            while (true) {
                LOGGER.info("Moving Event Count = {}", collector.getEventCountAsOfNow());
                if (countdownTimer.isFinished()) {
                    break;
                }
                TimeUnit.SECONDS.sleep(30);
            }
        } catch (final Throwable any) {
            LOGGER.error("Unexpected Error: Collecting Statistics", any);
        } finally {
            if (collector != null) {
                try {
                    // collect the final stats
                    LOGGER.info("Final Event Count = {}", collector.getEventCountAsOfNow());
                    LOGGER.info("Final Event Duplicate Count = {}", collector.getEventDuplicateCountAsOfNow());
                    collector.stopCollectingStatistics();
                } catch (final Throwable any) {
                    LOGGER.error("Unexpected Final Collection Error", any);
                }
            }
            // delete the executor
            if (executorService != null) {
                try {
                    executorService.shutdownNow();
                } catch (final Throwable ignore) {

                }
            }
            // delete the app table
            try {
                deleteDynamoTable(getAmazonDynamoDBClient(KinesisConnectorConfiguration.DEFAULT_DYNAMODB_ENDPOINT),
                        appName);
            } catch (final Throwable ignore) {
            }
            // kill everything
            System.exit(0);
        }
    }
}
