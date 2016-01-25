package com.dematic.labs.toolkit.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import org.junit.Test;

import static com.dematic.labs.toolkit.aws.Connections.CapacityUnit;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.deleteDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;
import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

/**
 * Test dyanamo db provisioning
 */
public class DynamoDBTest {

    @Test
    public void testCapacityUnitFromSystemProperties() {
        Long readCPU = 31L;
        Long writeCPU = 241L;
        String tableName = "testGetSystemProperties";
        String writePropertyName = Connections.getCapacitySystemPropertyName(tableName, CapacityUnit.WRITE_CAPACITY_UNITS);
        System.setProperty(writePropertyName, writeCPU.toString());
        assertEquals(writeCPU, Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.WRITE_CAPACITY_UNITS));
        System.clearProperty(writePropertyName);
        assertEquals(Connections.DEFAULT_DYNAMODB_DEFAULT_CAPACITY_UNITS,
                Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.WRITE_CAPACITY_UNITS));

        //clear and default
        String readPropertyName = Connections.getCapacitySystemPropertyName(tableName, CapacityUnit.READ_CAPACITY_UNITS);
        System.setProperty(readPropertyName, readCPU.toString());

        //clear and default
        assertEquals(readCPU, Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.READ_CAPACITY_UNITS));
        System.clearProperty(readPropertyName);
        assertEquals(Connections.DEFAULT_DYNAMODB_DEFAULT_CAPACITY_UNITS,
                Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.READ_CAPACITY_UNITS));

    }

    @Test
    public void testCreationWithCapacityAndUpdate() {
        Long readUnits = 3L;
        Long writeUnits = 5L;
        Long updatedReadUnits = 7L;
        Long updatedWriteUnits = 9L;
        final String userNamePrefix = System.getProperty("user.name") + "_";
        // need to set properties based on name
        String tableName = "Capacity_Test_" + userNamePrefix + Event.TABLE_NAME;
        String readPropertyName = Connections.getCapacitySystemPropertyName(tableName, CapacityUnit.READ_CAPACITY_UNITS);
        System.setProperty(readPropertyName, Long.toString(readUnits));

        String writePropertyName = Connections.getCapacitySystemPropertyName(tableName, CapacityUnit.WRITE_CAPACITY_UNITS);
        System.setProperty(writePropertyName, Long.toString(writeUnits));
        try {
            String generatedTableName = createDynamoTable(getDynamoDBEndPoint(), Event.class, "Capacity_Test_" + userNamePrefix);
            assertEquals(generatedTableName, tableName);


            final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient(getDynamoDBEndPoint());
            DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
            Table table = dynamoDB.getTable(tableName);
            assertNotNull(table);

            TableDescription description = table.describe();
            assertNotNull(description);
            assertEquals(readUnits, description.getProvisionedThroughput().getReadCapacityUnits());
            assertEquals(writeUnits, description.getProvisionedThroughput().getWriteCapacityUnits());

            // test update different values - do it one at a time to check or is working
            System.setProperty(readPropertyName, Long.toString(updatedReadUnits));
            createDynamoTable(getDynamoDBEndPoint(), Event.class, "Capacity_Test_" + userNamePrefix);
            description = table.describe();
            assertEquals(updatedReadUnits, description.getProvisionedThroughput().getReadCapacityUnits());

            System.setProperty(writePropertyName, Long.toString(updatedWriteUnits));
            createDynamoTable(getDynamoDBEndPoint(), Event.class, "Capacity_Test_" + userNamePrefix);
            description = table.describe();
            assertEquals(updatedWriteUnits, description.getProvisionedThroughput().getWriteCapacityUnits());

            // test update same values
            boolean changed = Connections.updateDynamoTableCapacity(dynamoDBClient,tableName, updatedReadUnits,updatedWriteUnits);
            assertFalse(changed);

        } finally {
            // delete the lease table, always in the east
            deleteDynamoTable(getAmazonDynamoDBClient(getDynamoDBEndPoint()), tableName);

        }
    }

    private String getDynamoDBEndPoint() {
        return System.getProperty("dynamoDBEndpoint");
    }

}
