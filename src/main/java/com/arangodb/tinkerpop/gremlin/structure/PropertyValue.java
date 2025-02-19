package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.shaded.fasterxml.jackson.annotation.JsonCreator;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonProperty;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

import java.util.Objects;

public class PropertyValue {

    private final Object value;
    private final String valueType;

    @JsonCreator
    public PropertyValue(
            @JsonProperty("value") Object value,
            @JsonProperty("valueType") String valueType
    ) {
        this.value = value;
        this.valueType = valueType;
    }

    public PropertyValue(Object value) {
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
        return "PropertyValue{" +
                "value=" + value +
                ", valueType='" + valueType + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PropertyValue that = (PropertyValue) o;
        return Objects.equals(value, that.value) && Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, valueType);
    }
}
    
