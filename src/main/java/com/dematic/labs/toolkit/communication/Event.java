package com.dematic.labs.toolkit.communication;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshalling;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Event needs to be defined,
 */
@SuppressWarnings("UnusedDeclaration")
@DynamoDBTable(tableName = Event.TABLE_NAME)
public final class Event implements Serializable {
    public static final String TABLE_NAME = "Events";

    private UUID id;
    private long sequence;
    private String nodeId; // Node-135
    private String jobId; // correlation Id
    private EventType type;
    private ReadableInstant timestamp; // time events are generated
    private String generatorId;
    private Long version;

    public Event() {
        sequence = EventSequenceNumber.next();
    }

    public Event(final UUID id, final long sequence, final String nodeId, final String jobId, final EventType type,
                 final ReadableInstant timestamp, final String generatorId, final Long version) {
        this.id = id;
        this.sequence = sequence;
        this.nodeId = nodeId;
        this.jobId = jobId;
        this.type = type;
        this.timestamp = timestamp;
        this.generatorId = generatorId;
        this.version = version;
    }

    @DynamoDBMarshalling(marshallerClass = UUIDMarshaller.class)
    @DynamoDBHashKey(attributeName = "id")
    public UUID getId() {
        return id;
    }

    public void setId(final UUID id) {
        this.id = id;
    }

    @DynamoDBAttribute
    public long getSequence() {
        return sequence;
    }

    public void setSequence(final long sequence) {
        this.sequence = sequence;
    }

    @DynamoDBAttribute
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(final String nodeId) {
        this.nodeId = nodeId;
    }

    @DynamoDBAttribute
    public String getJobId() {
        return jobId;
    }

    public void setJobId(final String jobId) {
        this.jobId = jobId;
    }

    @DynamoDBAttribute
    public EventType getType() {
        return type;
    }

    public void setType(final EventType type) {
        this.type = type;
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
    public String getGeneratorId() {
        return generatorId;
    }

    public void setGeneratorId(final String generatorId) {
        this.generatorId = generatorId;
    }

    @DynamoDBVersionAttribute
    public Long getVersion() {
        return version;
    }

    public void setVersion(final Long version) {
        this.version = version;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(sequence, event.sequence) &&
                Objects.equals(id, event.id) &&
                Objects.equals(nodeId, event.nodeId) &&
                Objects.equals(jobId, event.jobId) &&
                Objects.equals(type, event.type) &&
                Objects.equals(timestamp, event.timestamp) &&
                Objects.equals(generatorId, event.generatorId) &&
                Objects.equals(version, event.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, sequence, nodeId, jobId, type, timestamp, generatorId, version);
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", sequence=" + sequence +
                ", nodeId='" + nodeId + '\'' +
                ", jobId='" + jobId + '\'' +
                ", type='" + type + '\'' +
                ", timestamp=" + timestamp +
                ", generatorId='" + generatorId + '\'' +
                ", version=" + version +
                '}';
    }
}
