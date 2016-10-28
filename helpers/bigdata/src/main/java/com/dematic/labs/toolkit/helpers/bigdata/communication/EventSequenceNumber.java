package com.dematic.labs.toolkit.helpers.bigdata.communication;

import java.util.concurrent.atomic.AtomicLong;

public final class EventSequenceNumber {
    private static final AtomicLong SEQUENCE_NUMBER = new AtomicLong(1);

    private EventSequenceNumber() {
    }

    public static long next() {
        return SEQUENCE_NUMBER.getAndIncrement();
    }
}
