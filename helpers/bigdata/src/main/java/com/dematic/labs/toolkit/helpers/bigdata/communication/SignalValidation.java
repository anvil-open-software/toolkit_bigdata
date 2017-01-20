package com.dematic.labs.toolkit.helpers.bigdata.communication;

import java.util.Objects;

@SuppressWarnings("unused")
public final class SignalValidation {
    public static final String TABLE_NAME = "signal_validation";

    public static String createCounterTableCql(final String keyspace) {
        return String.format("CREATE TABLE if not exists %s.%s (" +
                " id text," +
                " producer_count counter," +
                " producer_error_count counter," +
                " spark_count counter," +
                " PRIMARY KEY (id));", keyspace, TABLE_NAME);
    }

    public static String createSparkTableCql(final String keyspace) {
        return String.format("CREATE TABLE if not exists %s.%s (" +
                " id text," +
                " spark_count text," +
                " PRIMARY KEY (id));", keyspace, TABLE_NAME);
    }

    private String id;
    private Long producerCount;
    private Long producerErrorCount;
    private Long sparkCount;

    public SignalValidation(final String id, final Long producerCount, final Long producerErrorCount,
                            final Long sparkCount) {
        this.id = id;
        this.producerCount = producerCount;
        this.producerErrorCount = producerErrorCount;
        this.sparkCount = sparkCount;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public Long getProducerCount() {
        return producerCount;
    }

    public void setProducerCount(final Long producerCount) {
        this.producerCount = producerCount;
    }

    public Long getProducerErrorCount() {
        return producerErrorCount;
    }

    public void setProducerErrorCount(final Long producerErrorCount) {
        this.producerErrorCount = producerErrorCount;
    }

    public Long getSparkCount() {
        return sparkCount;
    }

    public void setSparkCount(final Long sparkCount) {
        this.sparkCount = sparkCount;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SignalValidation that = (SignalValidation) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(producerCount, that.producerCount) &&
                Objects.equals(producerErrorCount, that.producerErrorCount) &&
                Objects.equals(sparkCount, that.sparkCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, producerCount, producerErrorCount, sparkCount);
    }

    @Override
    public String toString() {
        return "SignalValidation{" +
                "id='" + id + '\'' +
                ", producerCount=" + producerCount +
                ", producerErrorCount=" + producerErrorCount +
                ", sparkCount=" + sparkCount +
                '}';
    }
}