package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.serde.*;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonIgnore;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.stream.Stream;

public class ArangoDBEdgeData {

    @InternalKey
    private String key;

    @InternalRev
    private String rev;

    @InternalFrom
    private String from;

    @InternalTo
    private String to;

    @JsonProperty
    private String label;

    @JsonProperty
    private final Map<String, PropertyValue> properties = new HashMap<>();

    public ArangoDBEdgeData() {
    }

    public ArangoDBEdgeData(String label, String key, String from, String to) {
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

    public Set<String> keys() {
        return properties.keySet();
    }

    public Stream<Map.Entry<String, Object>> properties() {
        return properties.entrySet().stream()
                .map(it -> new AbstractMap.SimpleEntry<>(it.getKey(), it.getValue().getValue()));
    }

    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }

    public void removeProperty(String key) {
        properties.remove(key);
    }

    @JsonIgnore
    public void setProperty(String key, Object value) {
        properties.put(key, new PropertyValue(value));
    }

    @JsonIgnore
    public Object getProperty(String key) {
        return properties.get(key).getValue();
    }

    @Override
    public String toString() {
        return "ArangoDBEdgeDocument{" +
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
        ArangoDBEdgeData that = (ArangoDBEdgeData) o;
        return Objects.equals(key, that.key) && Objects.equals(rev, that.rev) && Objects.equals(from, that.from) && Objects.equals(to, that.to) && Objects.equals(label, that.label) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, rev, from, to, label, properties);
    }

}
