package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.shaded.fasterxml.jackson.annotation.JsonCreator;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ArangoDBVertexPropertyData extends ArangoDBPropertyData implements PropertiesContainer {
    private final String id;
    private final Map<String, ArangoDBPropertyData> properties;

    @JsonCreator
    ArangoDBVertexPropertyData(
            @JsonProperty("id") String id,
            @JsonProperty("value") Object value,
            @JsonProperty("valueType") String valueType,
            @JsonProperty("properties") Map<String, ArangoDBPropertyData> properties) {
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

    public Map<String, ArangoDBPropertyData> getProperties() {
        return properties;
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
