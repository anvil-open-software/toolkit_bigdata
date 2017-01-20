package com.dematic.labs.toolkit.helpers.bigdata.communication;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * VALID JSON (RFC 4627)
 * [{
 * "ExtendedProperties":[
 * ],
 * "ProxiedTypeName":"Odatech.Business.Integration.OPCTagReading",
 * "OPCTagID":1549,
 * "OPCTagReadingID":0,
 * "Quality":192,
 * "Timestamp":"2016-03-03T19:13:13.3980463Z",
 * "Value":"1995603996",
 * "ID":0,
 * "UniqueID":null
 * }]
 */
@SuppressWarnings({"UnusedDeclaration", "WeakerAccess"})
public final class Signal implements Serializable {
    public static final String TABLE_NAME = "signals";

    public static String createTableCql(final String keyspace) {
        return String.format("CREATE TABLE if not exists %s.%s (" +
                        " unique_id text, " +
                        " id bigint, " +
                        " value bigint, " +
                        " day text, " +
                        " timestamp timestamp, " +
                        " quality bigint, " +
                        " opc_tag_reading_id bigint, " +
                        " opc_tag_id bigint, " +
                        " proxied_type_name text, " +
                        " extended_properties list<text>, " +
                        " PRIMARY KEY ((opc_tag_id, day), timestamp)) WITH CLUSTERING ORDER BY (timestamp desc);",
                keyspace, TABLE_NAME);
    }

    private String uniqueId;
    private Long id;
    private Long value;
    private String day; // partition by day, form 2016-12-23
    private Date timestamp;
    private Long quality;
    private Long opcTagReadingId;
    private Long opcTagId;
    private String proxiedTypeName;
    private List<String> extendedProperties;

    public Signal() {
    }

    public Signal(final String uniqueId, final Long id, final Long value, final String day, final Date timestamp,
                  final Long quality, final Long opcTagReadingId, final Long opcTagId,
                  final String proxiedTypeName, final List<String> extendedProperties) {
        this.uniqueId = uniqueId;
        this.id = id;
        this.value = value;
        this.day = day;
        this.timestamp = timestamp;
        this.quality = quality;
        this.opcTagReadingId = opcTagReadingId;
        this.opcTagId = opcTagId;
        this.proxiedTypeName = proxiedTypeName;
        this.extendedProperties = extendedProperties;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(final String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public Long getId() {
        return id;
    }

    public void setId(final Long id) {
        this.id = id;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(final Long value) {
        this.value = value;
    }

    public String getDay() {
        return day;
    }

    public void setDay(final String day) {
        this.day = day;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final Date timestamp) {
        this.timestamp = timestamp;
    }

    public Long getQuality() {
        return quality;
    }

    public void setQuality(final Long quality) {
        this.quality = quality;
    }

    public Long getOpcTagReadingId() {
        return opcTagReadingId;
    }

    public void setOpcTagReadingId(final Long opcTagReadingId) {
        this.opcTagReadingId = opcTagReadingId;
    }

    public Long getOpcTagId() {
        return opcTagId;
    }

    public void setOpcTagId(final Long opcTagId) {
        this.opcTagId = opcTagId;
    }

    public String getProxiedTypeName() {
        return proxiedTypeName;
    }

    public void setProxiedTypeName(final String proxiedTypeName) {
        this.proxiedTypeName = proxiedTypeName;
    }

    public List<String> getExtendedProperties() {
        return extendedProperties;
    }

    public void setExtendedProperties(final List<String> extendedProperties) {
        this.extendedProperties = extendedProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Signal signal = (Signal) o;
        return Objects.equals(uniqueId, signal.uniqueId) &&
                Objects.equals(id, signal.id) &&
                Objects.equals(value, signal.value) &&
                Objects.equals(day, signal.day) &&
                Objects.equals(timestamp, signal.timestamp) &&
                Objects.equals(quality, signal.quality) &&
                Objects.equals(opcTagReadingId, signal.opcTagReadingId) &&
                Objects.equals(opcTagId, signal.opcTagId) &&
                Objects.equals(proxiedTypeName, signal.proxiedTypeName) &&
                Objects.equals(extendedProperties, signal.extendedProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueId, id, value, day, timestamp, quality, opcTagReadingId, opcTagId, proxiedTypeName, extendedProperties);
    }

    @Override
    public String toString() {
        return "Signal{" +
                "uniqueId='" + uniqueId + '\'' +
                ", id=" + id +
                ", value=" + value +
                ", day=" + day +
                ", timestamp=" + timestamp +
                ", quality=" + quality +
                ", opcTagReadingId=" + opcTagReadingId +
                ", opcTagId=" + opcTagId +
                ", proxiedTypeName='" + proxiedTypeName + '\'' +
                ", extendedProperties=" + extendedProperties +
                '}';
    }
}
