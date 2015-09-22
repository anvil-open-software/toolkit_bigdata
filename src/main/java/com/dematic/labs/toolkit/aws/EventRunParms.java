package com.dematic.labs.toolkit.aws;

import org.joda.time.DateTime;

import java.util.Arrays;


/**
 *
 *  Run input parameters which will be also persisted with the run stats
 */
public class EventRunParms {

    public EventRunParms(String[] rawArgs) {
        this.runStartTime = DateTime.now();
        this.rawArgs = rawArgs;
    }

    public String getKinesisInputStream() {
        return kinesisInputStream;
    }

    public void setKinesisInputStream(String kinesisInputStream) {
        this.kinesisInputStream = kinesisInputStream;
    }

    public String getDynamoDBEndPoint() {
        return dynamoEndPoint;
    }

    /**
     * todo pass it in somewhere
     * @param dynamoEndPoint
     */
    public void setDynamoDBEndPoint(String dynamoEndPoint) {
        this.dynamoEndPoint = dynamoEndPoint;
    }

    // todo default will be in east but make this configurable
    private String dynamoEndPoint = "http://dynamodb.us-east-1.amazonaws.com";
    private String kinesisInputStream;

    public DateTime getRunStartTime() {
        return runStartTime;
    }

    public DateTime getRunEndTime() {
        return runEndTime;
    }

    public void setRunEndTime(DateTime runEndTime) {
        this.runEndTime = runEndTime;
    }

    public long getDuration() {
        return duration;
    }

    /**
     * todo add timeunit...
     * @param duration
     */
    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String[] getRawArgs() {
        return rawArgs;
    }

    @Override
    public String toString() {
        return "EventRunParms{" +
                "dynamoEndPoint='" + dynamoEndPoint + '\'' +
                ", kinesisInputStream='" + kinesisInputStream + '\'' +
                ", runStartTime=" + runStartTime +
                ", runEndTime=" + runEndTime +
                ", rawArgs=" + Arrays.toString(rawArgs) +
                '}';
    }

    private DateTime runStartTime;
    private DateTime runEndTime;
    private long duration;
    private String[] rawArgs;

}
