package com.dematic.labs.toolkit.aws.summary;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;

import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;

/**
 * Summary row saved in DynamoDB after Kinesis Event Generator run is completed
 *
 *
 */

public class KinesisEventSummaryPersister {

    public KinesisEventSummaryPersister(String endpoint, String prefix) {
        this.endpoint = endpoint;
        this.prefix = prefix;
    }


    public void persistSummary(KinesisEventSummary summary) {
        // make sure table exists
        tableName = createDynamoTable(endpoint, KinesisEventSummary.class, prefix);

        // write the table. We need to override the table name mostly in the case of a test.
        DynamoDBMapper dynamoDBMapper = getMapper();
        DynamoDBMapperConfig config = new DynamoDBMapperConfig(new DynamoDBMapperConfig.TableNameOverride(tableName));
        dynamoDBMapper.save(summary,config);
    }

    public String getTableName() {
        return tableName;
    }

    private DynamoDBMapper getMapper() {
        if (dynamoDBMapper==null) {
            final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient(endpoint);
            dynamoDBMapper = new DynamoDBMapper(dynamoDBClient);
        }

        return dynamoDBMapper;
    }


    private String endpoint;
    private String prefix;
    private String tableName;
    private DynamoDBMapper dynamoDBMapper;

}

