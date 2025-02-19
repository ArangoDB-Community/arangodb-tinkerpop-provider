package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.serde.*;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

public class ArangoDBVertexData {

    @InternalKey
    private String key;

    @InternalRev
    private String rev;

    @JsonProperty
    private String label;

    @JsonProperty
    private Map<String, List<ArangoDBVertexPropertyData>> properties;

    public ArangoDBVertexData() {
    }

    public ArangoDBVertexData(String label, String key) {
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

    public Map<String, List<ArangoDBVertexPropertyData>> getProperties() {
        if (properties == null) {
            properties = new HashMap<>();
        }
        return properties;
    }

    public void setProperties(Map<String, List<ArangoDBVertexPropertyData>> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "ArangoDBVertexDocument{" +
                ", key='" + key + '\'' +
                ", rev='" + rev + '\'' +
                ", label='" + label + '\'' +
                ", properties=" + properties +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ArangoDBVertexData that = (ArangoDBVertexData) o;
        return Objects.equals(key, that.key) && Objects.equals(rev, that.rev) && Objects.equals(label, that.label) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, rev, label, properties);
    }

}

