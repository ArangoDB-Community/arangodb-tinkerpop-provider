package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.shaded.fasterxml.jackson.annotation.JsonCreator;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ArangoDBVertexPropertyData extends PropertyValue {
    private final String id;
    private final Map<String, PropertyValue> properties;

    @JsonCreator
    public ArangoDBVertexPropertyData(
            @JsonProperty("id") String id,
            @JsonProperty("value") Object value,
            @JsonProperty("valueType") String valueType,
            @JsonProperty("properties") Map<String, PropertyValue> properties) {
        super(value, valueType);
        this.id = id;
        this.properties = properties;
    }

    public ArangoDBVertexPropertyData(String id, Object value) {
        super(value);
        this.id = id;
        this.properties = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public Map<String, PropertyValue> getProperties() {
        return properties;
    }

    public void setProperty(String key, Object value) {
        properties.put(key, new PropertyValue(value));
    }

    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }

    public void removeProperty(String key) {
        properties.remove(key);
    }

    public Object getProperty(String key) {
        return properties.get(key).getValue();
    }

    @Override
    public String toString() {
        return "TinkerVertexPropertyData{" +
                "id='" + id + '\'' +
                ", value=" + getValue() +
                ", valueType='" + getValueType() + '\'' +
                ", properties=" + properties +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ArangoDBVertexPropertyData that = (ArangoDBVertexPropertyData) o;
        return Objects.equals(id, that.id) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), id, properties);
    }
}
