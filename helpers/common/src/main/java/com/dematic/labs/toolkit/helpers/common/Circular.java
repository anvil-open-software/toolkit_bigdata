package com.dematic.labs.toolkit.helpers.common;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.Monitor;

import java.util.Iterator;
import java.util.List;

public final class Circular<T> {
    private final Multiset<T> counter;
    private final Iterator<T> elements;
    private final Monitor monitor = new Monitor();

    public Circular(final List<T> elements) {
        counter = HashMultiset.create();
        this.elements = Iterables.cycle(elements).iterator();
    }

    public T getOne() {
        monitor.enter();
        try {
            final T element = this.elements.next();
            counter.add(element);
            return element;
        } finally {
            monitor.leave();
        }
    }

    public int getCount(final T element) {
        return counter.count(element);
    }
}
