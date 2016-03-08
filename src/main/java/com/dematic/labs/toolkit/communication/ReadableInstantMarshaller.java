package com.dematic.labs.toolkit.communication;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshaller;
import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;

public final class ReadableInstantMarshaller implements DynamoDBMarshaller<ReadableInstant> {
    @Override
    public String marshall(final ReadableInstant readableInstant) {
        return readableInstant.toString();
    }

    @Override
    public ReadableInstant unmarshall(final Class<ReadableInstant> clazz, String time) {
        return DateTime.parse(time);
    }
}
