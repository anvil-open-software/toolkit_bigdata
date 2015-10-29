package com.dematic.labs.toolkit.aws.kinesis.consumer;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.dematic.labs.toolkit.communication.Event;

public final class EventBasicMemoryBuffer extends BasicMemoryBuffer<Event> {
    public EventBasicMemoryBuffer(final KinesisConnectorConfiguration configuration) {
        super(configuration);
    }

    @Override
    public boolean shouldFlush() {
        return getRecords().size() > 0;
    }
}