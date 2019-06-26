package com.arangodb.tinkerpop.gremlin.velocipack;

import com.arangodb.tinkerpop.gremlin.structure.properties.ArngVertexProperty;
import com.arangodb.tinkerpop.gremlin.structure.properties.ElementProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.*;

public class ArngVPackVertexProperty implements VPackVertexProperty {

    private final String key;
    private final TinkerPopMetadata metadata;
    private final List<Object> values;
    private final VertexProperty.Cardinality cadinality;

    public ArngVPackVertexProperty(VertexProperty.Cardinality cardinality, VertexProperty value) {
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
        this(key, cardinality, values, new TinkerPopMetadata(cardinality, types, properties));
    }

    public ArngVPackVertexProperty(
            String key,
            VertexProperty.Cardinality cardinality,
            Collection<Object> values,
            TinkerPopMetadata metadata) {
        this.key = key;
        this.cadinality = cardinality;
        this.values = new ArrayList<>(values);
        this.metadata = metadata;
    }

    @SuppressWarnings("unchecked")
    @Override
    public VPackVertexProperty addVertexProperty(VertexProperty property) {
        if (!property.key().equals(key)) {
            throw new IllegalArgumentException(String.format(
                    "Only properties with the same key can be added. %s != %s",
                    key, property.key()));
        }
        List<Object> newValues = new ArrayList<>(values);
        newValues.add(property.value());
        List<ElementProperty> newProps = new ArrayList<>();
        property.properties().forEachRemaining(p -> newProps.add((ElementProperty)p));
        return new ArngVPackVertexProperty(
                key,
                cadinality,
                newValues,
                metadata.addMetadata(property.value().getClass().getCanonicalName(), newProps));
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public Object baseValue() {
        if (cadinality.equals(VertexProperty.Cardinality.single)) {
            return values.get(0);
        }
        else {
            return values;
        }
    }

//    @Override
//    public TinkerPopMetadata metadata() {
//        return metadata;
//    }
}
