package com.dematic.labs.toolkit.communication;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshaller;


public final class EventTypeMarshaller implements DynamoDBMarshaller<EventType> {
    @Override
    public String marshall(final EventType getterReturnResult) {
        return getterReturnResult.name();
    }

    @Override
    public EventType unmarshall(final Class<EventType> clazz, final String eventType) {
        return EventType.valueOf(eventType);
    }
}
