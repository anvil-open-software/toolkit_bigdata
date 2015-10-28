package com.dematic.labs.toolkit.aws.kinesis.consumer;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.dematic.labs.toolkit.CountdownTimer;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Multisets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.dematic.labs.toolkit.aws.Connections.*;

public final class EventStreamCollector extends KinesisConnectorExecutorBase<Event, byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamCollector.class);

    private final Multimap<UUID, Event> statistics;
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
        statistics = Multimaps.synchronizedMultimap(LinkedListMultimap.create());
        initialize(config, new NullMetricsFactory());
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<Event, byte[]> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(new EventStreamsConnectorPipeline(statistics), config);
    }

    public void startCollectingStatistics() {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(worker);
    }

    public void stopCollectingStatistics() {
        worker.shutdown();
    }

    public int getEventCountAsOfNow() {
        return statistics.keys().size();
    }

    public int getEventDuplicateCountAsOfNow() {
        final int[] sum = {0};
        final ImmutableMultiset<UUID> uuids = Multisets.copyHighestCountFirst(statistics.keys());
        uuids.entrySet().stream().forEach(uuid -> {
            final int count = uuid.getCount();
            if (count > 1) {
                sum[0] = sum[0] + count;
            } else {
                // just break out, if not duplicates
                //noinspection UnnecessaryReturnStatement
                return;
            }
        });
        return sum[0];
    }

    public static void main(String[] args) {
        EventStreamCollector collector = null;
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
            collector.startCollectingStatistics();
            // collect for specific amount of time in minutes
            final CountdownTimer countdownTimer = new CountdownTimer();
            countdownTimer.countDown(numberOfMinutes);
            while (true) {
                if (countdownTimer.isFinished()) {
                    LOGGER.info("Moving Event Count = {}", collector.getEventCountAsOfNow());
                    LOGGER.info("Moving Event Duplicate Count = {}", collector.getEventDuplicateCountAsOfNow());
                    break;
                }
            }
        } catch (final Throwable any) {
            LOGGER.error("Unexpected Error: Collecting Statistics", any);
        } finally {
            if (collector != null) {
                // collect the final stats
                LOGGER.info("Final Event Count = {}", collector.getEventCountAsOfNow());
                LOGGER.info("Final Event Duplicate Count = {}", collector.getEventDuplicateCountAsOfNow());
                try {
                    collector.stopCollectingStatistics();
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
