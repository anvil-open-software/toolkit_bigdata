package com.dematic.labs.toolkit.communication;

import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public final class EventTest {
    @Test
    public void convertEventToJson() throws IOException {
        // test event to json then to event
        final Event rawEvent = new Event(UUID.randomUUID(), EventSequenceNumber.next(), "Node-1", UUID.randomUUID(),
                EventType.UNKNOWN, DateTime.now(), "UnitTestGenerated", 1L);
        final String jsonEvent = EventUtils.eventToJson(rawEvent);
        final Event fromJson = EventUtils.jsonToEvent(jsonEvent);
        assertThat(rawEvent.getId(), is(fromJson.getId()));
        assertThat(rawEvent.getSequence(), is(fromJson.getSequence()));
        assertThat(rawEvent.getNodeId(), is(fromJson.getNodeId()));
        assertThat(rawEvent.getType(), is(fromJson.getType()));
        assertThat(rawEvent.getTimestamp(), is(fromJson.getTimestamp()));
        assertThat(rawEvent.getGeneratorId(), is(fromJson.getGeneratorId()));
        assertThat(rawEvent.getVersion(), is(fromJson.getVersion()));
    }

    @Test
    public void generateEvents() {
        final long numberOfEvents = 1000000;
        // test generation of events
        final List<Event> events = EventUtils.generateEvents(numberOfEvents, "Node-1");
        assertEquals(numberOfEvents, events.size());
    }
}
