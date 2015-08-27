package com.dematic.labs.toolkit.communication;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshalling;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Event needs to be defined,
 */
@SuppressWarnings("UnusedDeclaration")
@DynamoDBTable(tableName = Event.TABLE_NAME)
public final class Event implements Serializable {
    public static final String TABLE_NAME = "Events";

    private UUID eventId;
    private long sequence;
    private int nodeId; // node 1 - 5
    private int orderId; // job/order 1 - 9
    private ReadableInstant timestamp; // time events are generated
    private double value; // random value

    public Event() {
        sequence = EventSequenceNumber.next();
    }

    public Event(final UUID eventId, final long sequence, final int nodeId, final int orderId,
                 final ReadableInstant timestamp, final double value) {
        this.eventId = eventId;
        this.sequence = sequence;
        this.nodeId = nodeId;
        this.orderId = orderId;
        this.timestamp = timestamp;
        this.value = value;
    }

    @DynamoDBMarshalling(marshallerClass = UUIDMarshaller.class)
    @DynamoDBHashKey(attributeName = "id")
    public UUID getEventId() {
        return eventId;
    }

    public void setEventId(final UUID eventId) {
        this.eventId = eventId;
    }

    @DynamoDBAttribute
    public long getSequence() {
        return sequence;
    }

    public void setSequence(final long sequence) {
        this.sequence = sequence;
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

    public String aggregateBy(final TimeUnit unit) {
        final String aggregateTime;
        switch (unit) {
            case MINUTES: {
                aggregateTime = new DateTime(getTimestamp()).minuteOfHour().roundFloorCopy().toDateTimeISO().toString();
                break;
            }
            case HOURS: {
                aggregateTime = new DateTime(getTimestamp()).hourOfDay().roundFloorCopy().toDateTimeISO().toString();
                break;
            }
            default: {
                throw new IllegalArgumentException(String.format(">%s< needs to be either {MINUTES, HOURS}",
                        unit));
            }
        }
        return aggregateTime;
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventId=" + eventId +
                ", sequence=" + sequence +
                ", nodeId=" + nodeId +
                ", orderId=" + orderId +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
