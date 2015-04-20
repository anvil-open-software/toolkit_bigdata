package com.dematic.labs.toolkit.communication;

import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;

public final class EventTest {
    @Test
    public void convertEventToJson() throws IOException {
        // 1) test event to json then to event
        final Event rawEvent = new Event(UUID.randomUUID(), 1, 5, DateTime.now(), 1234.34);
        final String jsonEvent = EventUtils.eventToJson(rawEvent);
        final Event fromJson = EventUtils.jsonToEvent(jsonEvent);
        assertThat(rawEvent.getEventId(), is(fromJson.getEventId()));
        assertThat(rawEvent.getJobId(), is(fromJson.getJobId()));
        assertThat(rawEvent.getNodeId(), is(fromJson.getNodeId()));
        assertThat(rawEvent.getTimestamp(), is(fromJson.getTimestamp()));
        assertThat(rawEvent.getValue(), is(fromJson.getValue()));
    }
}
