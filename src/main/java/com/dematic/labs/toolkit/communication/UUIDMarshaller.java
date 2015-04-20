package com.dematic.labs.toolkit.communication;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshaller;

import java.util.UUID;

public final class UUIDMarshaller implements DynamoDBMarshaller<UUID> {
    @Override
    public String marshall(final UUID uuid) {
        return uuid.toString();
    }

    @Override
    public UUID unmarshall(final Class<UUID> clazz, final String uuid) {
        return UUID.fromString(uuid);
    }
}
