package com.dematic.labs.toolkit.aws.summary;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
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
@DynamoDBTable(tableName = KinesisEventSummary.KINESIS_EVENT_SUMMARY_TABLE_NAME)
public class KinesisEventSummary {

    public KinesisEventSummary(EventRunParms runParms) {
        this.runParms = runParms;

        try {
            user = System.getenv("USER");
            this.clientAddress = Inet4Address.getLocalHost().getHostAddress();
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
    public Long getSpecifiedDuration() {
        return runParms.getDuration();
    }


    @DynamoDBAttribute
    public Long getTotalEventsAttempted() {
        return totalEventsAttempted;
    }

    public void setTotalEventsAttempted(Long totalEventsAttempted) {
        this.totalEventsAttempted = totalEventsAttempted;
    }

    @DynamoDBAttribute
    public Long getTotalEventsAttemptedFailed() {
        return totalEventsAttemptedFailed;
    }

    public void setTotalEventsAttemptedFailed(Long totalEventsAttemptedFailed) {
        this.totalEventsAttemptedFailed = totalEventsAttemptedFailed;
    }

    @DynamoDBAttribute
    public Long getTotalEventsSucceeded() {
        return totalEventsSucceeded;
    }

    public void setTotalEventsSucceeded(Long totalEventsSucceeded) {
        this.totalEventsSucceeded = totalEventsSucceeded;
    }

    @DynamoDBAttribute
    public Long getTotalEventsFailed() {
        return totalEventsFailed;
    }

    public void setTotalEventsFailed(Long totalEventsFailed) {
        this.totalEventsFailed = totalEventsFailed;
    }

    @DynamoDBAttribute
    public Long getTotalEventsFailedSystemErrors() {
        return totalEventsFailedSystemErrors;
    }

    public void setTotalEventsFailedSystemErrors(Long totalEventsFailedSystemErrors) {
        this.totalEventsFailedSystemErrors = totalEventsFailedSystemErrors;
    }

    @DynamoDBAttribute
    public Long getTotalEventsFailedKinesisErrors() {
        return totalEventsFailedKinesisErrors;
    }

    public void setTotalEventsFailedKinesisErrors(Long totalEventsFailedKinesisErrors) {
        this.totalEventsFailedKinesisErrors = totalEventsFailedKinesisErrors;
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

        KinesisEventSummary that = (KinesisEventSummary) o;

        if (!getKinesisStream().equals(that.getKinesisStream())) return false;
        if (!getRunStartTime().equals(that.getRunStartTime())) return false;
        if (getRunEndTime() != null ? !getRunEndTime().equals(that.getRunEndTime()) : that.getRunEndTime() != null) return false;
        if (!version.equals(that.version)) return false;
        return getSpecifiedDuration().equals(that.getSpecifiedDuration());

    }

    @Override
    public int hashCode() {
        int result = getKinesisStream().hashCode();
        result = 31 * result + getRunStartTime().hashCode();
        result = 31 * result + (getRunEndTime() != null ? getRunEndTime().hashCode() : 0);
        result = 31 * result + version.hashCode();
        result = 31 * result + getSpecifiedDuration().hashCode();
        return result;
    }


    @Override
    public String toString() {
        return "KinesisEventSummary{" +
                " runParms=" + runParms +
                ", totalEventsAttempted=" + totalEventsAttempted +
                ", totalEventsAttemptedFailed=" + totalEventsAttemptedFailed +
                ", totalEventsSucceeded=" + totalEventsSucceeded +
                ", totalEventsFailed=" + totalEventsFailed +
                ", totalEventsFailedSystemErrors=" + totalEventsFailedSystemErrors +
                ", totalEventsFailedKinesisErrors=" + totalEventsFailedKinesisErrors +
                " version=" + version +
                '}';
    }

    public void logSummaryStats() {
        // old style logging
        LOGGER.info("Total Events ATTEMPTED: {}", getTotalEventsAttempted());
        LOGGER.info("Total Events SUCCEEDED: {}",  getTotalEventsSucceeded());
        LOGGER.info("Total Events FAILED: {}",getTotalEventsFailed());
        LOGGER.info("Total System Events FAILED: {}", getTotalEventsFailedSystemErrors());
        LOGGER.info("Total Kinesis Events FAILED: {}", getTotalEventsFailedKinesisErrors());
    }

    /**
     * DO NOT rename this to a getter. Eventhough it is not marked for being a DynamoDBAttribute, it gets picked up
     * and will fail the write.
     * @return
     */
    public EventRunParms fetchRunParms() {
        return runParms;
    }

    public static final String KINESIS_EVENT_SUMMARY_TABLE_NAME = "perftest_kinesis_event_summary";

    private Long version;
    private Long totalEventsAttempted;
    private Long totalEventsAttemptedFailed;
    private Long totalEventsSucceeded;
    private Long totalEventsFailed;
    private Long totalEventsFailedSystemErrors;
    private Long totalEventsFailedKinesisErrors;

    private String clientAddress;
    private EventRunParms runParms;
    private String user;
    private String rawJavaAppParms;
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventSummaryPersister.class);

}
