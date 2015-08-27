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
        final Event rawEvent = new Event(UUID.randomUUID(), EventSequenceNumber.next(), 1, 5, DateTime.now(), 1234.34);
        final String jsonEvent = EventUtils.eventToJson(rawEvent);
        final Event fromJson = EventUtils.jsonToEvent(jsonEvent);
        assertThat(rawEvent.getEventId(), is(fromJson.getEventId()));
        assertThat(rawEvent.getSequence(), is(fromJson.getSequence()));
        assertThat(rawEvent.getOrderId(), is(fromJson.getOrderId()));
        assertThat(rawEvent.getNodeId(), is(fromJson.getNodeId()));
        assertThat(rawEvent.getTimestamp(), is(fromJson.getTimestamp()));
        assertThat(rawEvent.getValue(), is(fromJson.getValue()));
    }

    @Test
    public void generateEvents() {
        final long numberOfEvents = 1000000;
        // test generation of events
        final List<Event> events = EventUtils.generateEvents(numberOfEvents, 20, 50);
        assertEquals(numberOfEvents, events.size());
    }
}
