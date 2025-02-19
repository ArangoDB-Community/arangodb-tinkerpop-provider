package com.arangodb.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil.elementAlreadyRemoved;

public class ArangoDBVertexProperty<V> implements Element, VertexProperty<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDBVertexProperty.class);

    private final String key;
    private final ArangoDBVertex vertex;
    private final ArangoDBVertexPropertyData data;
    private boolean removed;

    public ArangoDBVertexProperty(String key, ArangoDBVertexPropertyData data, ArangoDBVertex vertex) {
        this.key = key;
        this.data = data;
        this.vertex = vertex;
        removed = false;
    }

    @Override
    public String key() {
        return key;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V value() throws NoSuchElementException {
        return (V) data.getValue();
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return vertex;
    }


    @Override
    public Object id() {
        return data.getId();
    }

    @Override
    public <W> Property<W> property(String key, W value) {
        if (removed) throw elementAlreadyRemoved(VertexProperty.class, id());
        LOGGER.info("set property {} = {}", key, value);
        ElementHelper.validateProperty(key, value);
        data.setProperty(key, value);
        vertex.update();
        return new ArangoDBProperty<>(this, key, value);
    }

    @Override
    public void remove() {
        if (removed) return;
        vertex.removeProperty(data);
        vertex.update();
        removed = true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return data.getProperties()
                .entrySet()
                .stream()
                .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                .map(entry -> (Property<U>) new ArangoDBProperty<>(this, entry.getKey(), entry.getValue().getValue()))
                .collect(Collectors.toList()).iterator();
    }

    public void removeProperty(String key) {
        if (removed) throw elementAlreadyRemoved(Edge.class, id());
        if (data.hasProperty(key)) {
            data.removeProperty(key);
            vertex.update();
        }
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Property<?>) this);
    }

}
