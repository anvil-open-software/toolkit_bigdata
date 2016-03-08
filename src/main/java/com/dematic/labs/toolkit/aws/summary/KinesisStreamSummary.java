package com.dematic.labs.toolkit.aws.summary;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.dematic.labs.toolkit.aws.EventRunParms;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.util.Arrays;


/**
 * Summary Row in Dynamodb is created after Kinesis Event Generator run is completed
 *
 * Note this class can be written to dynamo but not read in from dynamo due to complex EventRunParms
 */

@SuppressWarnings("unused")
@DynamoDBTable(tableName = KinesisStreamSummary.KINESIS_STREAM_SUMMARY_TABLE_NAME)
public class KinesisStreamSummary {

    public KinesisStreamSummary(EventRunParms runParms) {

        try {
             user = System.getenv("USER");
             clientAddress = Inet4Address.getLocalHost().getHostAddress();
             this.runParms = runParms;


        } catch (Throwable e) {
            // go on since this field is only for diagnostics
            LOGGER.error("Failed to get machine address: " + e);
        }
    }


    @DynamoDBHashKey()
    public String getKinesisStream() {
        return runParms.getKinesisInputStream();
    }

    @DynamoDBVersionAttribute
    public Long getVersion() {
        return version;
    }

    @DynamoDBRangeKey
    public String getRunStartTime() {
        // convert date
        return runParms.getRunStartTime().toDateTimeISO().toString();
    }

    public void setVersion(final Long version) {
        this.version = version;
    }

    @DynamoDBAttribute
    public Long getRunStartTimeInMillis() {
        return runParms.getRunStartTime().getMillis();
    }

    @DynamoDBAttribute
    public Long getRunEndTimeInMillis() {
        DateTime endTime = runParms.getRunEndTime();
        if (endTime != null) {
            return endTime.getMillis();
        } else return null;
    }


    @DynamoDBAttribute
    public String getRunEndTime() {
        DateTime endTime = runParms.getRunEndTime();
        if (endTime != null) {
            return endTime.toDateTimeISO().toString();
        } else return null;
    }

    @DynamoDBAttribute
    public String getClientAddress() {
        return clientAddress;
    }

    @DynamoDBAttribute
    public String getUser() {
        return user;
    }

    /**
     *
     * todo either add duration time unit or normalize
     */
    @DynamoDBAttribute
    public Long getDurationInMinutes() {
        return runParms.getDuration();
    }


    @DynamoDBAttribute
    public Long getTotalEventCount() {
        return totalEventCount;
    }

    public void setTotalEventCount(Long totalEventCount) {
        this.totalEventCount = totalEventCount;
    }

    @DynamoDBAttribute
    public Long getTotalDuplicateEventCount() {
        return totalDuplicateEventCount;
    }

    public void setTotalDuplicateEventCount(Long totalDuplicateEventCount) {
        this.totalDuplicateEventCount = totalDuplicateEventCount;
    } 


    /**
     * preserve the run arguments in case we are looking at performance
     * @return
     */
    @DynamoDBAttribute
    public String getRawJavaArgs() {
        return Arrays.toString(fetchRunParms().getRawArgs());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KinesisStreamSummary that = (KinesisStreamSummary) o;

        if (!getKinesisStream().equals(that.getKinesisStream())) return false;
        if (!getRunStartTime().equals(that.getRunStartTime())) return false;
        if (getRunEndTime() != null ? !getRunEndTime().equals(that.getRunEndTime()) : that.getRunEndTime() != null) return false;
        if (!version.equals(that.version)) return false;
        return getDurationInMinutes().equals(that.getDurationInMinutes());

    }

    @Override
    public int hashCode() {
        int result = getKinesisStream().hashCode();
        result = 31 * result + getRunStartTime().hashCode();
        result = 31 * result + (getRunEndTime() != null ? getRunEndTime().hashCode() : 0);
        result = 31 * result + version.hashCode();
        result = 31 * result + getDurationInMinutes().hashCode();
        return result;
    }


    @Override
    public String toString() {
        return "KinesisEventSummary{" +
                " runParms=" + runParms +
                ", totalEventCount=" + totalEventCount +
                ", totalDuplicateEventCount=" + totalDuplicateEventCount +
                " version=" + version +
                '}';
    }


    /**
     * DO NOT rename this to a getter. Eventhough it is not marked for being a DynamoDBAttribute, it gets picked up
     * and will fail the write.
     * @return
     */
    public EventRunParms fetchRunParms() {
        return runParms;
    }

    public static final String KINESIS_STREAM_SUMMARY_TABLE_NAME = "perftest_kinesis_stream_summary";

    private Long version;
    private Long totalEventCount;
    private Long totalDuplicateEventCount;
    private String clientAddress;
    private EventRunParms runParms;
    private String user;
    private String rawJavaAppParms;
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventSummaryPersister.class);

}
