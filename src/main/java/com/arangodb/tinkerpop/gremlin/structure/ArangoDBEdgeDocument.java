package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.serde.*;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonCreator;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonProperty;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ArangoDBEdgeDocument {

    @InternalKey
    private String key;

    @InternalRev
    private String rev;

    @InternalFrom
    private String from;

    @InternalTo
    private String to;

    private String label;

    private final Map<String, TinkerValue> properties = new HashMap<>();

    public ArangoDBEdgeDocument() {
    }

    public ArangoDBEdgeDocument(String label, String key, String from, String to) {
        this.label = label;
        this.key = key;
        this.from = from;
        this.to = to;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getRev() {
        return rev;
    }

    public void setRev(String rev) {
        this.rev = rev;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Map<String, TinkerValue> getProperties() {
        return properties;
    }

    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }

    public void removeProperty(String key) {
        properties.remove(key);
    }

    public void setProperty(String key, Object value) {
        properties.put(key, new TinkerValue(value));
    }

    public Object getProperty(String key) {
        return properties.get(key).getValue();
    }

    @Override
    public String toString() {
        return "TinkerEdgeDocument{" +
                ", key='" + key + '\'' +
                ", rev='" + rev + '\'' +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", label='" + label + '\'' +
                ", properties=" + properties +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ArangoDBEdgeDocument that = (ArangoDBEdgeDocument) o;
        return Objects.equals(key, that.key) && Objects.equals(rev, that.rev) && Objects.equals(from, that.from) && Objects.equals(to, that.to) && Objects.equals(label, that.label) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, rev, from, to, label, properties);
    }

    public static class TinkerValue {
        private final Object value;
        private final String valueType;

        @JsonCreator
        public TinkerValue(
                @JsonProperty("value") Object value,
                @JsonProperty("valueType") String valueType
        ) {
            this.value = value;
            this.valueType = valueType;
        }

        public TinkerValue(Object value) {
            this.value = value;
            valueType = (value != null ? value.getClass() : Void.class).getCanonicalName();
        }

        public Object getValue() {
            return ArangoDBUtil.getCorretctPrimitive(value, valueType);
        }

        public String getValueType() {
            return valueType;
        }

        @Override
        public String toString() {
            return "TinkerValue{" +
                    "value=" + value +
                    ", valueType='" + valueType + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            TinkerValue that = (TinkerValue) o;
            return Objects.equals(value, that.value) && Objects.equals(valueType, that.valueType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, valueType);
        }
    }

}
