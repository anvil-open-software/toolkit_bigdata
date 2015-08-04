package com.dematic.labs.toolkit.aws;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.Metric;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.dematic.labs.toolkit.Circular;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.dematic.labs.toolkit.aws.Connections.getAmazonKinesisClient;
import static com.dematic.labs.toolkit.communication.EventUtils.eventToJsonByteArray;
import static com.dematic.labs.toolkit.communication.EventUtils.generateEvents;

/**
 * Client will push generated events to a kinesis stream.
 *
 *
 * NOTE: does not handle any failures, that is, will just log and continue
 */
public class KinesisEventClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventClient.class);

    private KinesisEventClient() {
    }

    public static void pushEventsToKinesis(final String kinesisEndpoint, final String kinesisInputStream,
                                           final List<Event> events) {
        final AmazonKinesisClient amazonKinesisClient = getAmazonKinesisClient(kinesisEndpoint);
        final DescribeStreamResult describeStreamResult = amazonKinesisClient.describeStream(kinesisInputStream);
        final List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

        final KinesisProducerConfiguration kinesisProducerConfiguration = new KinesisProducerConfiguration();
        // get the region from the URL
        kinesisProducerConfiguration.setRegion(RegionUtils.getRegionByEndpoint(kinesisEndpoint).getName());
        // max connection is 128
        kinesisProducerConfiguration.setMaxConnections(128);
        final KinesisProducer kinesisProducer = new KinesisProducer(kinesisProducerConfiguration);

        // move to the next shard in the list
        final Circular<String> partitionKey = new Circular<>(partitionKeys(shards.size(), 100));
        events.stream()
                .parallel()
                .forEach(event -> {
                            try {
                                final ListenableFuture<UserRecordResult> userRecordResult =
                                        kinesisProducer.addUserRecord(kinesisInputStream, partitionKey.getOne(),
                                                ByteBuffer.wrap(eventToJsonByteArray(event)));
                                LOGGER.debug("event sent to shard >{}<", userRecordResult.get().getShardId());
                            } catch (final IOException | InterruptedException | ExecutionException ioe) {
                                LOGGER.error("unable to push event >{}< to the kinesis stream", event, ioe);
                            }
                        }
                );
        try {
            kinesisProducer.flushSync();
            final List<Metric> metrics = kinesisProducer.getMetrics();
            for (final Metric metric : metrics) {
                LOGGER.info(metric.toString());
            }
        } catch (final InterruptedException | ExecutionException ioe) {
            LOGGER.error("unable to get metrics", ioe);
        } finally {
            kinesisProducer.destroy();
        }
    }

    private static List<String> partitionKeys(final int numberOfShards, final int partitionKeyBuffer) {
        // As a result of this hashing mechanism, all data records with the same partition key map to the same shard within the stream.
        // However, if the number of partition keys exceeds the number of shards, some shards necessarily contain records with different
        // partition keys. From a design standpoint, to ensure that all your shards are well utilized, the number of shards
        // (specified by the setShardCount method of CreateStreamRequest) should be substantially less than the number of unique
        // partition keys, and the amount of data flowing to a single partition key should be substantially less than the capacity of the shard.
        final List<String> partitionKeys = Lists.newArrayList();
        for (int i = 0; i < numberOfShards + partitionKeyBuffer; i++) {
            partitionKeys.add("pk-" + i);
        }
        return partitionKeys;
    }

    public static void main(final String[] args) {
        if (args == null || args.length != 5) {
            throw new IllegalArgumentException(
                    "ensure all the following are set {kinesisEndpoint, kinesisInputStream, numberOfEvents, nodeSize, " +
                            "orderSize");
        }
        final String kinesisEndpoint = args[0];
        final String kinesisInputStream = args[1];

        final long numberOfEvents = Long.valueOf(args[2]);
        final int nodeSize = Integer.valueOf(args[3]);
        final int orderSize = Integer.valueOf(args[4]);

        //generate events and push to Kinesis
        pushEventsToKinesis(kinesisEndpoint, kinesisInputStream, generateEvents(numberOfEvents, nodeSize, orderSize));
    }
}
