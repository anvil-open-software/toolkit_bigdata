package com.dematic.labs.toolkit.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

import static com.amazonaws.util.StringUtils.isNullOrEmpty;

public final class Connections {
    private static final Logger LOGGER = LoggerFactory.getLogger(Connections.class);

    private Connections() {
    }

    public static AWSCredentialsProvider getAWSCredentialsProvider() {
        // AWS credentials are set in system properties via junit.properties
        return new DefaultAWSCredentialsProviderChain();
    }

    public static AmazonKinesisClient getAmazonKinesisClient(final String awsEndpointUrl) {
        final AmazonKinesisClient kinesisClient = new AmazonKinesisClient(getAWSCredentialsProvider());
        kinesisClient.setEndpoint(awsEndpointUrl);
        return kinesisClient;
    }

    /**
     * Creates an async kinesis client.
     *
     * @param awsEndpointUrl -- aws endpoint
     * @param parallelism    -- creates a thread pool that maintains enough threads to support the given parallelism
     *                       level, and may use multiple queues to reduce contention. The parallelism level corresponds
     *                       to the maximum number of threads actively engaged in, or available to engage in, task
     *                       processing. The actual number of threads may grow and shrink dynamically. A work-stealing
     *                       pool makes no guarantees about the order in which submitted tasks are executed.
     * @return AmazonKinesisAsyncClient
     */
    public static AmazonKinesisAsyncClient getAmazonAsyncKinesisClient(final String awsEndpointUrl, final int parallelism) {
        final AmazonKinesisAsyncClient kinesisClient = new AmazonKinesisAsyncClient(getAWSCredentialsProvider(),
                Executors.newWorkStealingPool(parallelism));
        kinesisClient.setEndpoint(awsEndpointUrl);
        return kinesisClient;
    }

    public static void createKinesisStreams(final AmazonKinesisClient kinesisClient, final String kinesisStream,
                                            final int kinesisStreamShardCount) {
        if (kinesisStreamsExist(kinesisClient, kinesisStream)) {
            final String state = kinesisStreamState(kinesisClient, kinesisStream);
            if (state != null) {
                switch (state) {
                    case "DELETING":
                        final long startTime = System.currentTimeMillis();
                        final long endTime = startTime + 1000 * 120;
                        while (System.currentTimeMillis() < endTime && kinesisStreamsExist(kinesisClient, kinesisStream)) {
                            try {
                                LOGGER.info("...deleting Stream " + kinesisStream + "...");
                                Thread.sleep(1000 * 10);
                            } catch (final InterruptedException ignore) {
                            }
                        }
                        if (kinesisStreamsExist(kinesisClient, kinesisStream)) {
                            LOGGER.error("kinesisUtils timed out waiting for stream " + kinesisStream + " to delete");
                            throw new IllegalStateException("KinesisUtils timed out waiting for stream " + kinesisStream
                                    + " to delete");
                        }
                    case "ACTIVE":
                        LOGGER.info("Stream " + kinesisStream + " is ACTIVE");
                        return;
                    case "CREATING":
                        break;
                    case "UPDATING":
                        LOGGER.info("stream " + kinesisStream + " is UPDATING");
                        return;
                    default:
                        throw new IllegalStateException("illegal stream state: " + state);
                }
            }
        } else {
            final CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(kinesisStream);
            createStreamRequest.setShardCount(kinesisStreamShardCount);
            kinesisClient.createStream(createStreamRequest);
            LOGGER.info("stream " + kinesisStream + " created");
        }
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(1000 * 10);
            } catch (final Exception ignore) {
            }
            try {
                final String streamStatus = kinesisStreamState(kinesisClient, kinesisStream);
                if (streamStatus != null && streamStatus.equals("ACTIVE")) {
                    LOGGER.info("stream " + kinesisStream + " is ACTIVE");
                    return;
                }
            } catch (final ResourceNotFoundException ignore) {
                throw new IllegalStateException("stream " + kinesisStream + " never went active");
            }
        }
    }

    public static boolean kinesisStreamsExist(final AmazonKinesisClient kinesisClient, final String kinesisStream) {
        try {
            kinesisClient.describeStream(kinesisStream);
            return true;
        } catch (final Throwable ignore) {
            return false;
        }
    }

    private static String kinesisStreamState(final AmazonKinesisClient kinesisClient, final String streamName) {
        final DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        try {
            return kinesisClient.describeStream(describeStreamRequest).getStreamDescription().getStreamStatus();
        } catch (final AmazonServiceException ignore) {
            return null;
        }
    }

    public static void deleteKinesisStream(final AmazonKinesisClient kinesisClient, final String kinesisStream) {
        kinesisClient.deleteStream(kinesisStream);
    }

    public static void deleteDynamoTable(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        if (dynamoTableExists(dynamoDBClient, tableName)) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
            deleteTableRequest.setTableName(tableName);
            dynamoDBClient.deleteTable(deleteTableRequest);
            LOGGER.info("deleted table " + tableName);
        } else {
            LOGGER.warn("table " + tableName + " does not exist");
        }
    }

    public static void deleteDynamoLeaseManagerTable(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        deleteDynamoTable(dynamoDBClient, tableName);
    }

    public static AmazonDynamoDBClient getAmazonDynamoDBClient(final String awsEndpointUrl) {
        final AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(getAWSCredentialsProvider());
        dynamoDBClient.setEndpoint(awsEndpointUrl);
        return dynamoDBClient;
    }

    public static String createDynamoTable(final String awsEndpointUrl, final Class<?> clazz, final String tablePrefix) {
        final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient(awsEndpointUrl);
        final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(dynamoDBClient);
        final CreateTableRequest createTableRequest = dynamoDBMapper.generateCreateTableRequest(clazz);
        final String tableName = isNullOrEmpty(tablePrefix) ? createTableRequest.getTableName() :
                String.format("%s%s", tablePrefix, createTableRequest.getTableName());
        if (dynamoTableExists(dynamoDBClient, tableName)) {
            waitForActive(dynamoDBClient, tableName);
            return tableName;
        }
        try {
            // just using default read/write provisioning, will need to use a service to monitor and scale accordingly
            createTableRequest.setTableName(tableName);
            createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(10L, 10L));
            dynamoDBClient.createTable(createTableRequest);
        } catch (com.amazonaws.services.autoscaling.model.ResourceInUseException e) {
            throw new IllegalStateException("the table may already be getting created.", e);
        }
        LOGGER.info("table " + tableName + " created");
        waitForActive(dynamoDBClient, tableName);
        return tableName;
    }

    public static boolean dynamoTableExists(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        try {
            dynamoDBClient.describeTable(describeTableRequest);
            return true;
        } catch (Throwable ignore) {
            return false;
        }
    }

    private static void waitForActive(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        switch (getTableStatus(dynamoDBClient, tableName)) {
            case DELETING:
                throw new IllegalStateException("table " + tableName + " is in the DELETING state");
            case ACTIVE:
                LOGGER.info("table " + tableName + " is ACTIVE");
                return;
            default:
                long startTime = System.currentTimeMillis();
                long endTime = startTime + (10 * 60 * 1000);
                while (System.currentTimeMillis() < endTime) {
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (final InterruptedException ignore) {
                    }
                    try {
                        if (getTableStatus(dynamoDBClient, tableName) == TableStatus.ACTIVE) {
                            LOGGER.info("table " + tableName + " is ACTIVE");
                            return;
                        }
                    } catch (final ResourceNotFoundException ignore) {
                        throw new IllegalStateException("table " + tableName + " never went active");
                    }
                }
        }
    }

    public static TableStatus getTableStatus(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        final DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        final DescribeTableResult describeTableResult = dynamoDBClient.describeTable(describeTableRequest);
        final String status = describeTableResult.getTable().getTableStatus();
        return TableStatus.fromValue(status);
    }
}
