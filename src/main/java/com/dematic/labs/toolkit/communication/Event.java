package com.dematic.labs.toolkit.communication;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshalling;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import org.joda.time.ReadableInstant;

import java.util.UUID;

/**
 * Event needs to be defined,
 */
@SuppressWarnings("UnusedDeclaration")
@DynamoDBTable(tableName = Event.TABLE_NAME)
public final class Event {
    public static final String TABLE_NAME = "Events";

    private UUID eventId;
    private int nodeId; // node 1 - 5
    private int orderId; // job/order 1 - 9
    private ReadableInstant timestamp; // time events are generated
    private double value; // random value

    public Event() {
    }

    public Event(final UUID eventId, final int nodeId, final int orderId, final ReadableInstant timestamp, final double value) {
        this.eventId = eventId;
        this.nodeId = nodeId;
        this.orderId = orderId;
        this.timestamp = timestamp;
        this.value = value;
    }

    @DynamoDBMarshalling(marshallerClass = UUIDMarshaller.class)
    @DynamoDBHashKey(attributeName="id")
    public UUID getEventId() {
        return eventId;
    }

    public void setEventId(final UUID eventId) {
        this.eventId = eventId;
    }

    @DynamoDBAttribute
    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(final int nodeId) {
        this.nodeId = nodeId;
    }

    @DynamoDBAttribute
    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(final int orderId) {
        this.orderId = orderId;
    }

    @DynamoDBMarshalling(marshallerClass = ReadableInstantMarshaller.class)
    @DynamoDBRangeKey
    public ReadableInstant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final ReadableInstant timestamp) {
        this.timestamp = timestamp;
    }

    @DynamoDBAttribute
    public double getValue() {
        return value;
    }

    public void setValue(final double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventId=" + eventId +
                ", nodeId=" + nodeId +
                ", orderId=" + orderId +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
