package com.dematic.labs.toolkit.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.jayway.awaitility.Awaitility;
import org.joda.time.DateTime;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.toolkit.aws.Connections.*;
import static org.junit.Assert.assertTrue;

public final class KinesisStreamRule extends ExternalResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisStreamRule.class);

    @Override
    protected void before() throws Throwable {
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        final AmazonKinesisClient kinesisClient = getAmazonKinesisClient(kinesisEndpoint);
        // create the kinesis stream and ensure active
        createKinesisStreams(kinesisClient, kinesisInputStream, 1);
        // set the defaults
        Awaitility.setDefaultTimeout(3, TimeUnit.MINUTES);
        // now poll
        Awaitility.with().pollInterval(2, TimeUnit.SECONDS).and().with().
                pollDelay(30, TimeUnit.SECONDS).await().
                until(() -> assertTrue(kinesisStreamsExist(kinesisClient, kinesisInputStream)));
    }

    @Override
    protected void after() {
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        final AmazonKinesisClient kinesisClient = getAmazonKinesisClient(kinesisEndpoint);
        try {
            // delete the stream
            deleteKinesisStream(kinesisClient, kinesisInputStream);
            // ensure stream removed
            // set the defaults
            Awaitility.setDefaultTimeout(3, TimeUnit.MINUTES);
            // now poll
            Awaitility.with().pollInterval(2, TimeUnit.SECONDS).and().with().
                    pollDelay(30, TimeUnit.SECONDS).await().
                    until(() -> assertTrue(!kinesisStreamsExist(kinesisClient, kinesisInputStream)));
        } catch (final Throwable any) {
            LOGGER.error(String.format("error deleting stream >%s<",kinesisInputStream), any);
        }
        // delete the dynamo db lease table created using spark's streaming, the lease table is always within the east region
        final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient("https://dynamodb.us-east-1.amazonaws.com");
        // todo: make table configurable
        deleteDynamoLeaseManagerTable(dynamoDBClient, Event.TABLE_NAME);
        // ensure table removed
        // now poll
        Awaitility.with().pollInterval(2, TimeUnit.SECONDS).and().with().
                pollDelay(30, TimeUnit.SECONDS).await().
                until(() -> assertTrue(!dynamoTableExists(dynamoDBClient, Event.TABLE_NAME)));
    }

    public void generateEventsAndPush(final int count) throws IOException {
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        // push events to a Kinesis stream
        for (int i = 1; i <= count; i++) {
            final PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(kinesisInputStream);
            final Event event = new Event(UUID.randomUUID(), i, i, DateTime.now(), i);
            putRecordRequest.setData(ByteBuffer.wrap(EventUtils.eventToJsonByteArray(event)));
            // partition key = which shard to send the request,
            putRecordRequest.setPartitionKey("1");
            final PutRecordResult putRecordResult = getAmazonKinesisClient(kinesisEndpoint).putRecord(putRecordRequest);
            LOGGER.info("pushed event >{}< : status: {}", event.getEventId(), putRecordResult.toString());
        }
    }
}
