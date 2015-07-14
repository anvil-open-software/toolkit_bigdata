package com.dematic.labs.toolkit.aws;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.dematic.labs.toolkit.Circular;
import com.dematic.labs.toolkit.communication.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.dematic.labs.toolkit.aws.Connections.getAmazonKinesisClient;
import static com.dematic.labs.toolkit.communication.EventUtils.eventToJsonByteArray;
import static com.dematic.labs.toolkit.communication.EventUtils.generateEvents;

public class KinesisEventClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventClient.class);

    private KinesisEventClient() {
    }

    public static void pushEventsToKinesis(final String kinesisEndpoint, final String kinesisInputStream,
                                           final List<Event> events) {
        final AmazonKinesisClient amazonKinesisClient = getAmazonKinesisClient(kinesisEndpoint);
        final DescribeStreamResult describeStreamResult = amazonKinesisClient.describeStream(kinesisInputStream);
        final List<Shard> shards = describeStreamResult.getStreamDescription().getShards();
        // move to the next shard in the list
        final Circular<Shard> shardCircular = new Circular<>(shards);
        events.stream()
                .parallel()
                .forEach(event -> {
                    final PutRecordRequest putRecordRequest = new PutRecordRequest();
                    putRecordRequest.setStreamName(kinesisInputStream);
                    try {
                        putRecordRequest.setData(ByteBuffer.wrap(eventToJsonByteArray(event)));
                        // move to the next shard
                        final String shardId = shardCircular.getOne().getShardId();
                        putRecordRequest.setPartitionKey(shardId);
                        final PutRecordResult putRecordResult =
                                amazonKinesisClient.putRecord(putRecordRequest);
                        LOGGER.info("pushed event >{}< : status: {}", event.getEventId(), putRecordResult.toString());
                    } catch (final IOException ioe) {
                        LOGGER.error("unable to push event >{}< to the kinesis stream", event, ioe);
                    }
                });
    }

    public static void main(final String[] args) {
        if(args == null || args.length > 5) {
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
