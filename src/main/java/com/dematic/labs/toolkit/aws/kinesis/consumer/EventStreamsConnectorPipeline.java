package com.dematic.labs.toolkit.aws.kinesis.consumer;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.*;
import com.amazonaws.services.kinesis.model.Record;

import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

import static com.dematic.labs.toolkit.communication.EventUtils.eventToJsonByteArray;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

public final class EventStreamsConnectorPipeline implements IKinesisConnectorPipeline<Event, byte[]> {
    private final Multimap<UUID, byte[]> statistics;

    public EventStreamsConnectorPipeline(final Multimap<UUID, byte[]> statistics) {
        this.statistics = statistics;
    }

    @Override
    public IEmitter<byte[]> getEmitter(final KinesisConnectorConfiguration configuration) {
        return new EventEmitter(statistics);
    }

    @Override
    public IBuffer<Event> getBuffer(final KinesisConnectorConfiguration configuration) {
        return new EventBasicMemoryBuffer(configuration);
    }

    @Override
    public ITransformerBase<Event, byte[]> getTransformer(final KinesisConnectorConfiguration configuration) {
        return new ITransformer<Event, byte[]>() {
            @Override
            public Event toClass(final Record record) throws IOException {
                return jsonToEvent(new String(record.getData().array(), Charset.defaultCharset()));
            }

            @Override
            public byte[] fromClass(final Event record) throws IOException {
                return eventToJsonByteArray(record);
            }
        };
    }

    @Override
    public IFilter<Event> getFilter(final KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<>();
    }
}
