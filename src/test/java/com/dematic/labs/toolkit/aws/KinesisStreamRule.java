package com.dematic.labs.toolkit.aws;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.dematic.labs.toolkit.aws.kinesis.KinesisEventClient;
import com.dematic.labs.toolkit.communication.Event;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionTimeoutException;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.toolkit.aws.Connections.*;
import static com.dematic.labs.toolkit.communication.EventUtils.generateEvents;
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
                pollDelay(10, TimeUnit.SECONDS).await().
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
                    pollDelay(10, TimeUnit.SECONDS).await().
                    until(() -> assertTrue(!kinesisStreamsExist(kinesisClient, kinesisInputStream)));
        } catch (final Throwable any) {
            LOGGER.error(String.format("error deleting stream >%s<", kinesisInputStream), any);
        }
    }

    public boolean pushEventsToKinesis(final int batchSize, final int nodeSize, final int orderSize,
                                       final long timeValue, final TimeUnit unit) {
        try {
            Awaitility.waitAtMost(timeValue, unit).until(() -> {
                pushEventsToKinesis(generateEvents(batchSize, nodeSize, orderSize));
                LOGGER.info("pushed >{}< events to kinesis", batchSize);
                return false;
            });
        } catch (final ConditionTimeoutException ignore) {
            // we've reached the maximum time allocated
        }
        // completed
        return true;
    }

    public void pushEventsToKinesis(final List<Event> events) {
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        KinesisEventClient.pushEventsToKinesis(kinesisEndpoint, kinesisInputStream, events);
    }
}
