package com.dematic.labs.toolkit.aws;

import static java.util.stream.StreamSupport.stream;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class FixedBatchSpliterator<T> extends FixedBatchSpliteratorBase<T> {
    private final Spliterator<T> spliterator;

    public FixedBatchSpliterator(final Spliterator<T> toWrap, final int batchSize, final long est) {
        super(toWrap.characteristics(), batchSize, est);
        this.spliterator = toWrap;
    }

    public FixedBatchSpliterator(final Spliterator<T> toWrap, int batchSize) {
        this(toWrap, batchSize, toWrap.estimateSize());
    }

    public static <T> Stream<T> withBatchSize(final Stream<T> in, int batchSize) {
        return stream(new FixedBatchSpliterator<>(in.spliterator(), batchSize), true);
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        return spliterator.tryAdvance(action);
    }

    @Override
    public void forEachRemaining(final Consumer<? super T> action) {
        spliterator.forEachRemaining(action);
    }
}