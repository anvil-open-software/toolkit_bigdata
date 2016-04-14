package com.dematic.labs.toolkit.communication;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * VALID JSON (RFC 4627)
 [{
 "ExtendedProperties":[
 ],
 "ProxiedTypeName":"Odatech.Business.Integration.OPCTagReading",
 "OPCTagID":1549,
 "OPCTagReadingID":0,
 "Quality":192,
 "Timestamp":"2016-03-03T19:13:13.3980463Z",
 "Value":"1995603996",
 "ID":0,
 "UniqueID":null
 }]
 */
public final class Signal implements Serializable {
    private String uniqueId;
    private String id;
    private String value;
    private String timestamp;
    private String quality;
    private String opcTagReadingId;
    private String opcTagId;
    private String proxiedTypeName;
    private List<String> extendedProperties;

    public Signal() {
    }

    public Signal(final String uniqueId, final String id, final String value, final String timestamp,
                  final String quality, final String opcTagReadingId, final String opcTagId,
                  final String proxiedTypeName, final List<String> extendedProperties) {
        this.uniqueId = uniqueId;
        this.id = id;
        this.value = value;
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

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final String timestamp) {
        this.timestamp = timestamp;
    }

    public String getQuality() {
        return quality;
    }

    public void setQuality(final String quality) {
        this.quality = quality;
    }

    public String getOpcTagReadingId() {
        return opcTagReadingId;
    }

    public void setOpcTagReadingId(final String opcTagReadingId) {
        this.opcTagReadingId = opcTagReadingId;
    }

    public String getOpcTagId() {
        return opcTagId;
    }

    public void setOpcTagId(final String opcTagId) {
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
                Objects.equals(timestamp, signal.timestamp) &&
                Objects.equals(quality, signal.quality) &&
                Objects.equals(opcTagReadingId, signal.opcTagReadingId) &&
                Objects.equals(opcTagId, signal.opcTagId) &&
                Objects.equals(proxiedTypeName, signal.proxiedTypeName) &&
                Objects.equals(extendedProperties, signal.extendedProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueId, id, value, timestamp, quality, opcTagReadingId, opcTagId, proxiedTypeName,
                extendedProperties);
    }

    @Override
    public String toString() {
        return "Signal{" +
                "uniqueId='" + uniqueId + '\'' +
                ", id='" + id + '\'' +
                ", value='" + value + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", quality='" + quality + '\'' +
                ", opcTagReadingID='" + opcTagReadingId + '\'' +
                ", opcTagID='" + opcTagId + '\'' +
                ", proxiedTypeName='" + proxiedTypeName + '\'' +
                ", extendedProperties=" + extendedProperties +
                '}';
    }
}
