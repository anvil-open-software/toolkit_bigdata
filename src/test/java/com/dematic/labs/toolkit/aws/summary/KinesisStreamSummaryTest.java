package com.dematic.labs.toolkit.aws.summary;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.dematic.labs.toolkit.aws.EventRunParms;
import org.joda.time.DateTime;
import org.junit.Test;

import static com.dematic.labs.toolkit.aws.Connections.deleteDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

/**
 * just crud test
 */

public class KinesisStreamSummaryTest {

    @Test
    public void testEntry() {
        final Long finalCount = 1234101L;
        final Long duplicates = 2L;
        final String userNamePrefix = System.getProperty("user.name") + "_";
        String[] inRawArgs = null;
        EventRunParms runParms = new EventRunParms(inRawArgs);
        runParms.setDynamoDBEndPoint(getDynamoDBEndPoint());
        KinesisStreamSummaryPersister persister = new KinesisStreamSummaryPersister(runParms.getDynamoDBEndPoint(), userNamePrefix);

        try {
            runParms.setKinesisInputStream("test_stream_summary");
            runParms.setRunEndTime(DateTime.now());
            DateTime endTime = DateTime.now();
            KinesisStreamSummary summary = new KinesisStreamSummary(runParms);
            summary.setTotalEventCount(finalCount);
            summary.setTotalDuplicateEventCount(duplicates);
            runParms.setRunEndTime(endTime);
            persister.persistSummary(summary);

            // check to see that it's there

            Item persistedSummary = getSummaryRow(persister.getTableName(), summary.getKinesisStream(),
                    summary.getRunStartTime());
            assertNotNull(persistedSummary);
            assertEquals(summary.getRunEndTime(), persistedSummary.getString("runEndTime"));
            assertEquals(finalCount,Long.valueOf(persistedSummary.getLong("totalEventCount")));
            assertEquals(duplicates,Long.valueOf(persistedSummary.getLong("totalDuplicateEventCount")));
            assertEquals(Long.valueOf(endTime.getMillis()),Long.valueOf(persistedSummary.getLong("runEndTimeInMillis")));

        } finally {
            deleteDynamoTable(getAmazonDynamoDBClient(runParms.getDynamoDBEndPoint()), persister.getTableName());
        }
    }

    protected String getDynamoDBEndPoint() {
        return System.getProperty("dynamoDBEndpoint");
    }

    public Item getSummaryRow(String inTable, String inKey, String inRange) {
        final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient(getDynamoDBEndPoint());
        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
        Table table = dynamoDB.getTable(inTable);
        Item item= table.getItem("kinesisStream", inKey, "runStartTime", inRange);
        return item;
    }
}
