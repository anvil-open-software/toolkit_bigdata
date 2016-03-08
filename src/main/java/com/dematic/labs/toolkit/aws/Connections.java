package com.dematic.labs.toolkit.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.google.common.base.Strings;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.Executors;

import static com.amazonaws.util.StringUtils.isNullOrEmpty;

public final class Connections {
    private static final Logger LOGGER = LoggerFactory.getLogger(Connections.class);
    public static final Long DEFAULT_DYNAMODB_DEFAULT_CAPACITY_UNITS = 10L;
    public enum CapacityUnit { READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS }
    public static final String DYNAMODB_SYSTEM_PROP_PREFIX = "dlabs.aws.dynamodb.";

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

    public static AmazonKinesisClient getAmazonKinesisClient(final String awsEndpointUrl,
                                                             final ClientConfiguration clientConfiguration) {
        final AmazonKinesisClient kinesisClient = new AmazonKinesisClient(getAWSCredentialsProvider(),
                clientConfiguration);
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
                new ClientConfiguration().withMaxConnections(150).withRetryPolicy(PredefinedRetryPolicies.NO_RETRY_POLICY),
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

    public static int getNumberOfShards(final String awsEndpointUrl, final String streamName) {
        final AmazonKinesisClient amazonKinesisClient = getAmazonKinesisClient(awsEndpointUrl);
        // Determine the number of shards from the stream and create 1 Kinesis Worker/Receiver/DStream for each shard
        return amazonKinesisClient.describeStream(streamName).getStreamDescription().getShards().size();
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

    /**
     *
     * @param tableName
     * @param capacityType
     * @return  capacityUnit from 1. system properties specific for table if defined 2. global if defined
     * 3. otherwise DEFAULT_DYNAMODB_DEFAULT_CAPACITY_UNITS
     */
    public static Long getDynamoDBTableCapacityUnits(String tableName, CapacityUnit capacityType) {
        String propertyName= getCapacitySystemPropertyName(tableName,capacityType);
        String capacityUnitsStr = System.getProperty(propertyName);
        Long capUnits = DEFAULT_DYNAMODB_DEFAULT_CAPACITY_UNITS;
        if (!Strings.isNullOrEmpty(capacityUnitsStr) ) {
            capUnits =  Long.valueOf(capacityUnitsStr);
        } else { // try global setting
            String globalCapacityUnitsStr = System.getProperty( DYNAMODB_SYSTEM_PROP_PREFIX + capacityType.name());
            if (!Strings.isNullOrEmpty(globalCapacityUnitsStr) ) {
                capUnits = Long.valueOf(globalCapacityUnitsStr);
            }
        }
        LOGGER.info("DynamoDB capacity units " + propertyName + "=" + capUnits);
        return capUnits;
    }

    /**
     *
     * @return append names together to get system property,
     * i.e. dlabs.aws.dynamodb.your_table_name.READ_CAPACITY_UNITS
     */
    public static String getCapacitySystemPropertyName(String tableName, CapacityUnit capacityType) {
        return  DYNAMODB_SYSTEM_PROP_PREFIX + tableName + "." + capacityType.name();
    }

    /**
     *
     * update provision if new values are different
     */
   public static boolean updateDynamoTableCapacity(AmazonDynamoDBClient dynamoDBClient, String tableName,
                                                Long readCapacityUnits,Long writeCapacityUnits) {
       boolean changedCapacity = false;
       DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
       Table table = dynamoDB.getTable(tableName);

       // check to see if the new capacity values and old values differ
       try {
           TableDescription description = table.describe();
           ProvisionedThroughputDescription throughput = description.getProvisionedThroughput();
           if (throughput.getReadCapacityUnits().longValue() != readCapacityUnits.longValue()
                   || throughput.getWriteCapacityUnits().longValue() != writeCapacityUnits.longValue()) {
               // we will need to update since values differ
               // see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/JavaDocumentAPITablesExample.html
               LOGGER.info("Modifying specified provisioned throughput for " + tableName);
               LOGGER.warn("read units: new =" + readCapacityUnits + " old=" + throughput.getReadCapacityUnits());
               LOGGER.warn("write units: new =" + writeCapacityUnits+ " old=" + throughput.getWriteCapacityUnits());

               table.updateTable(new ProvisionedThroughput()
                       .withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(writeCapacityUnits));
               // waitForActive has it's own try catch but needs to be in this if then statement and
               waitForActive(dynamoDBClient, tableName);
           }

       } catch (Exception e) {
           LOGGER.error("UpdateTable request failed for " + tableName);
           LOGGER.error(e.getMessage());
       }

       return changedCapacity;
   }
    /**
     *
     * @param awsEndpointUrl
     * @param clazz
     * @param tablePrefix
     * @return
     */
    public static String createDynamoTable(final String awsEndpointUrl, final Class<?> clazz, final String tablePrefix) {
        final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient(awsEndpointUrl);
        final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(dynamoDBClient);
        final CreateTableRequest createTableRequest = dynamoDBMapper.generateCreateTableRequest(clazz);
        final String tableName = isNullOrEmpty(tablePrefix) ? createTableRequest.getTableName() :
                String.format("%s%s", tablePrefix, createTableRequest.getTableName());

        Long readCapacityUnits = getDynamoDBTableCapacityUnits(tableName, CapacityUnit.READ_CAPACITY_UNITS);
        Long writeCapcityUnits =  getDynamoDBTableCapacityUnits(tableName, CapacityUnit.WRITE_CAPACITY_UNITS);

        if (dynamoTableExists(dynamoDBClient, tableName)) {
            waitForActive(dynamoDBClient, tableName);
            // update throughput if needed
            updateDynamoTableCapacity(dynamoDBClient, tableName, readCapacityUnits, writeCapcityUnits);

            return tableName;
        }
        try {
            // just using default read/write provisioning, will need to use a service to monitor and scale accordingly
            createTableRequest.setTableName(tableName);
            createTableRequest.setProvisionedThroughput(new ProvisionedThroughput( readCapacityUnits, writeCapcityUnits));
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

    public static JedisPool getCacheClientPool(final String url, final Integer port) {
        final GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(50);
        return port == null ? new JedisPool(genericObjectPoolConfig, url) :
                new JedisPool(genericObjectPoolConfig, url, port);
    }
}
