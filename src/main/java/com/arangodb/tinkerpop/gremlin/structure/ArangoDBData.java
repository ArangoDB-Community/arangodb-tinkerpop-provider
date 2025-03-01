package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.serde.InternalKey;
import com.arangodb.serde.InternalRev;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

abstract class ArangoDBData<T> {
    private String label;

    @InternalKey
    private String key;

    @InternalRev
    private String rev;

    private Map<String, T> properties = new HashMap<>();

    public ArangoDBData() {
    }

    public ArangoDBData(String label, String key) {
        Objects.requireNonNull(label, "label");
        if (label.isEmpty()) throw new IllegalArgumentException("empty label");
        if (key != null && key.isEmpty()) throw new IllegalArgumentException("empty key");

        this.label = label;
        this.key = key;
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

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Map<String, T> getProperties() {
        if (properties == null) {
            properties = new HashMap<>();
        }
        return properties;
    }

    public void setProperties(Map<String, T> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "key='" + key + '\'' +
                ", label='" + label + '\'' +
                ", rev='" + rev + '\'' +
                ", properties=" + properties;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ArangoDBData<?> that = (ArangoDBData<?>) o;
        return Objects.equals(label, that.label) && Objects.equals(key, that.key) && Objects.equals(rev, that.rev) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, key, rev, properties);
    }
}
