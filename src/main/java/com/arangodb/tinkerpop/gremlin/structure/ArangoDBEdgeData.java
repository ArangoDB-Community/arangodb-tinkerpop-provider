package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.serde.*;

import java.util.*;

public class ArangoDBEdgeData extends ArangoDBData<ArangoDBPropertyData> implements PropertiesContainer {

    @InternalFrom
    private String from;

    @InternalTo
    private String to;

    public ArangoDBEdgeData() {
    }

    public ArangoDBEdgeData(
            String label,
            String key,
            String from,
            String to
    ) {
        super(label, key);
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");

        this.from = from;
        this.to = to;
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

    @Override
    public String toString() {
        return "ArangoDBEdgeData{" +
                super.toString() +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", properties=" + getProperties() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ArangoDBEdgeData that = (ArangoDBEdgeData) o;
        return Objects.equals(from, that.from) && Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), from, to);
    }
}
