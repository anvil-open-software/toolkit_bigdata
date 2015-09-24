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

public class KinesisEventSummaryTest {

    @Test
    public void testEntry() {
        final Long attemtpsSucceeded = 100323l;
        final Long attemptsFailed = 1290l;
        final String userNamePrefix = System.getProperty("user.name") + "_";
        KinesisEventSummaryPersister persister = new KinesisEventSummaryPersister(getDynamoDBEndPoint(), userNamePrefix);
        String[] inRawArgs = null;
        try {
            EventRunParms runParms = new EventRunParms(inRawArgs);
            runParms.setKinesisInputStream("test_stream");
            runParms.setRunEndTime(DateTime.now());
            KinesisEventSummary summary = new KinesisEventSummary(runParms);
            summary.setTotalEventsFailed(attemptsFailed);
            summary.setTotalEventsSucceeded(attemtpsSucceeded);
            summary.setTotalEventsFailedKinesisErrors(333l);
            persister.persistSummary(summary);

            // check to see that it's there

            Item persistedSummary = getSummaryRow(persister.getTableName(), summary.getKinesisStream(),
                    summary.getRunStartTime());
            assertNotNull(persistedSummary);
            assertEquals(summary.getRunEndTime(), persistedSummary.getString("runEndTime"));
            assertEquals(attemptsFailed,Long.valueOf(persistedSummary.getLong("totalEventsFailed")));
            assertEquals(attemtpsSucceeded,Long.valueOf(persistedSummary.getLong("totalEventsSucceeded")));


        } finally {
            deleteDynamoTable(getAmazonDynamoDBClient(getDynamoDBEndPoint()), persister.getTableName());
            // delete the lease table, always in the east
        }
    }

    private String getDynamoDBEndPoint() {
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
