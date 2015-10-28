package com.dematic.labs.toolkit.aws.kinesis.consumer;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

public final class EventEmitter implements IEmitter<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventEmitter.class);

    private final Multimap<UUID, Event> statistics;

    public EventEmitter(final Multimap<UUID, Event> statistics) {
        this.statistics = statistics;
    }

    @Override
    public List<byte[]> emit(final UnmodifiableBuffer<byte[]> buffer) throws IOException {
        final List<byte[]> failed = new LinkedList<>();
        buffer.getRecords().stream().forEach(record -> {
            try {
                final Event event = jsonToEvent(new String(record, Charset.defaultCharset()));
                final boolean put = statistics.put(event.getEventId(), event);
                if (!put) {
                    LOGGER.error("Error writing record to output stream. Failing this emit attempt. Record: " +
                            Arrays.toString(record));
                    failed.add(record);
                }
            } catch (final IOException ioe) {
                LOGGER.error("Error writing record to output stream. Failing this emit attempt. Record: " +
                        Arrays.toString(record), ioe);
                failed.add(record);
            }
        });
      return failed;
    }

    @Override
    public void fail(List<byte[]> records) {
        //todo: deal wilth
        throw new IllegalStateException("failed records");
    }

    @Override
    public void shutdown() {
        LOGGER.info("shutting down...");
    }
}
