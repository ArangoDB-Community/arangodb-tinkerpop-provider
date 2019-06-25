package com.arangodb.tinkerpop.gremlin.velocipack;

import com.arangodb.tinkerpop.gremlin.structure.properties.ArngVertexProperty;
import com.arangodb.tinkerpop.gremlin.structure.properties.ElementProperties;
import com.arangodb.tinkerpop.gremlin.structure.properties.ElementProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.*;

public class ArngVPackVertexProperty implements VPackVertexProperty {

    private final String key;
    private final TinkerPopMetadata metadata;
    private final List<Object> values;

    public ArngVPackVertexProperty(VertexProperty.Cardinality cardinality, ArngVertexProperty value) {
        this(value.key(), cardinality,
                Collections.singletonList(value.value()),
                Collections.singletonList(value.getClass().getCanonicalName()),
                value.properties());
    }

    public ArngVPackVertexProperty(
            String key,
            VertexProperty.Cardinality cardinality,
            Collection<Object> values,
            Collection<String> types,
            Iterator<ElementProperty> properties) {
        this(key, values, new TinkerPopMetadata(cardinality, types, properties));
    }

    public ArngVPackVertexProperty(
            String key,
            Collection<Object> values,
            TinkerPopMetadata metadata) {
        this.key = key;
        this.values = new ArrayList<>(values);
        this.metadata = metadata;
    }

    @Override
    public ArngVPackVertexProperty addPropertyInformation(ArngVertexProperty property) {
        if (!property.key().equals(key)) {
            throw new IllegalArgumentException("Property can only be added to ArngVPackVertexProperty with same key.");
        }
        List<Object> newValues = new ArrayList<>(values);
        newValues.add(property.value());
        List<ElementProperty> newProps = new ArrayList<>();
        property.properties().forEachRemaining(p -> newProps.add((ElementProperty)p));
        return new ArngVPackVertexProperty(key, newValues, metadata.addMetadata(property.value().getClass().getCanonicalName(), newProps));
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public Object baseValue() {
        if (values.size() == 1) {
            return values.get(0);
        }
        else {
            return values;
        }
    }

    @Override
    public TinkerPopMetadata metadata() {
        return metadata;
    }
}
