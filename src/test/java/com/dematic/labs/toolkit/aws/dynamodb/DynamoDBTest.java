package com.dematic.labs.toolkit.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import org.junit.Rule;
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

    @Rule
    public final SystemPropertyRule systemPropertyRule = new SystemPropertyRule();

    private void clearGlobalCapacity() {
        System.clearProperty(Connections.DYNAMODB_SYSTEM_PROP_PREFIX + CapacityUnit.WRITE_CAPACITY_UNITS );
        System.clearProperty(Connections.DYNAMODB_SYSTEM_PROP_PREFIX + CapacityUnit.READ_CAPACITY_UNITS );
    }
    private void setGlobalCapacity(CapacityUnit capacityUnitType,Long capacityUnit) {
        if (capacityUnit==null) {
            System.clearProperty(Connections.DYNAMODB_SYSTEM_PROP_PREFIX + capacityUnitType);
        }
         else {
            System.setProperty(Connections.DYNAMODB_SYSTEM_PROP_PREFIX + capacityUnitType,Long.toString(capacityUnit));
        }
    }
    private void assertEqualsDefaultCapacity(String inTableName, CapacityUnit capacityUnitType) {
        assertEquals(Connections.DEFAULT_DYNAMODB_DEFAULT_CAPACITY_UNITS,
                Connections.getDynamoDBTableCapacityUnits(inTableName, capacityUnitType));
    }
    @Test
    public void testCapacityUnitFromGlobalSystemProperties() {

        Long globalReadCPU = 2L;
        Long globalWriteCPU = 7L;
        String tableName = "testCapacityUnitFromGlobalSystemProperties";

        clearGlobalCapacity();
        assertEqualsDefaultCapacity(tableName, CapacityUnit.WRITE_CAPACITY_UNITS);
        assertEqualsDefaultCapacity(tableName, CapacityUnit.READ_CAPACITY_UNITS);

        // set global
        setGlobalCapacity(CapacityUnit.WRITE_CAPACITY_UNITS, globalWriteCPU);
        assertEquals(globalWriteCPU, Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.WRITE_CAPACITY_UNITS));

        setGlobalCapacity(CapacityUnit.READ_CAPACITY_UNITS,globalReadCPU);
        assertEquals(globalReadCPU, Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.READ_CAPACITY_UNITS));

    }

    @Test
    public void testCapacityUnitFromSystemProperties() {

        // keep numbers low so we don't have to pay..
        Long readCPU = 3L;
        Long writeCPU = 5L;
        Long globalReadCPU = 8L;
        Long globalWriteCPU = 6L;
        String tableName = "testGetSystemProperties";

        clearGlobalCapacity();

        // set table specific
        String writePropertyName = Connections.getCapacitySystemPropertyName(tableName, CapacityUnit.WRITE_CAPACITY_UNITS);
        System.setProperty(writePropertyName, writeCPU.toString());
        assertEquals(writeCPU, Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.WRITE_CAPACITY_UNITS));

        // set global but it still has to be the table specific not the global
        setGlobalCapacity(CapacityUnit.WRITE_CAPACITY_UNITS, globalWriteCPU);
        assertEquals(writeCPU, Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.WRITE_CAPACITY_UNITS));

        System.clearProperty(writePropertyName);
        clearGlobalCapacity();
        assertEqualsDefaultCapacity(tableName, CapacityUnit.WRITE_CAPACITY_UNITS);

        //set table level
        String readPropertyName = Connections.getCapacitySystemPropertyName(tableName, CapacityUnit.READ_CAPACITY_UNITS);
        System.setProperty(readPropertyName, readCPU.toString());
        assertEquals(readCPU, Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.READ_CAPACITY_UNITS));
        // set global and it still has to be the table specific not the global
        setGlobalCapacity(CapacityUnit.READ_CAPACITY_UNITS, globalReadCPU);
        assertEquals(readCPU, Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.READ_CAPACITY_UNITS));

        //clear and default
        System.clearProperty(readPropertyName);
        assertEquals(globalReadCPU, Connections.getDynamoDBTableCapacityUnits(tableName, CapacityUnit.READ_CAPACITY_UNITS));
        clearGlobalCapacity();
        assertEqualsDefaultCapacity(tableName, CapacityUnit.READ_CAPACITY_UNITS);
    }

    @Test
    public void testTableCreationWithCapacityAndUpdate() {
        Long readUnits = 3L;
        Long writeUnits = 5L;
        Long updatedReadUnits = 7L;
        Long updatedWriteUnits = 9L;
        Long globalReadCPU = 11L;
        Long globalWriteCPU = 12L;
        final String userNamePrefix = System.getProperty("user.name") + "_";
        // need to set properties based on name
        String tableName = "Capacity_Test_" + userNamePrefix + Event.TABLE_NAME;
        String readPropertyName = Connections.getCapacitySystemPropertyName(tableName, CapacityUnit.READ_CAPACITY_UNITS);
        System.setProperty(readPropertyName, Long.toString(readUnits));
        setGlobalCapacity(CapacityUnit.WRITE_CAPACITY_UNITS, globalWriteCPU);
        String writePropertyName = Connections.getCapacitySystemPropertyName(tableName, CapacityUnit.WRITE_CAPACITY_UNITS);
        System.setProperty(writePropertyName, Long.toString(writeUnits));
        setGlobalCapacity(CapacityUnit.READ_CAPACITY_UNITS, globalReadCPU);

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

            // test global
            System.clearProperty(writePropertyName);
            System.clearProperty(readPropertyName);
            createDynamoTable(getDynamoDBEndPoint(), Event.class, "Capacity_Test_" + userNamePrefix);
            description = table.describe();
            assertEquals(globalWriteCPU, description.getProvisionedThroughput().getWriteCapacityUnits());
            assertEquals(globalReadCPU, description.getProvisionedThroughput().getReadCapacityUnits());

        } finally {
            // delete the lease table, always in the east
            deleteDynamoTable(getAmazonDynamoDBClient(getDynamoDBEndPoint()), tableName);

        }
    }

    private String getDynamoDBEndPoint() {
        return System.getProperty("dynamoDBEndpoint");
    }

}
