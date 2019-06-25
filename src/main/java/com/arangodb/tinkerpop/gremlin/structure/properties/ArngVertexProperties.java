package com.arangodb.tinkerpop.gremlin.structure.properties;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;

/**
 * An implementation of {@link VertexProperties} for an {@link ArangoDBVertex} that uses a {@link HashMap} to store
 * value information for each key.
 *
 * The stored values are instances of {@link ArngVertexPropertyValue}.
 *
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public class ArngVertexProperties implements VertexProperties {

    /** The element that owns the property */

    protected final ArangoDBVertex vertex;

    /** The value(s) of the elementProperties, keyd by property primaryKey (name) */

    protected final Map<String, VertexPropertyValue> properties = new HashMap<>();

    public ArngVertexProperties(ArangoDBVertex element) {
        this.vertex = element;
    }

    @Override
    public Set<String> keys() {
        return Collections.unmodifiableSet(properties.keySet());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> VertexProperty<V> property(final String key) {
        return properties.get(key).one(key);
    }

    /**
     * Get the {@link VertexProperty} for the provided primaryKey. If the property does not exist, return
     * {@link VertexProperty#empty}.
     *
     * @param <V>                   the expected type of the vertex property value
     * @param key                   the primaryKey of the vertex property to get
     * @return                      the retrieved vertex property
     */
    @Override
    public <V> VertexProperty<V> property(
        final String key,
        final V value) {
        final ArngVertexProperty<V> property = new ArngVertexProperty<>(key, value, vertex);
        properties.put(key, new ArngVertexPropertyValue<>(VertexProperty.Cardinality.single, property));
        return property;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<V> values(String... propertyKeys) {
        List<Iterator<V>> iterators = new ArrayList<>();
        allPropertiesIfEmpty(propertyKeys).forEachRemaining(k -> iterators.add(properties.get(k).values()));
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public <V> VertexProperty<V> property(
        VertexProperty.Cardinality cardinality,
        String key,
        V value,
        Object... keyValues) {
        ElementHelper.validateProperty(key, value);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (cardinality.equals(VertexProperty.Cardinality.single)) {
            final ArngVertexProperty<V> property = new ArngVertexProperty<>(key, value, vertex);
            properties.put(key, new ArngVertexPropertyValue<>(VertexProperty.Cardinality.single, property));
            ElementHelper.attachProperties(property, keyValues);
            return property;
        }
        else {
            try {
                return multivalue(key, value, cardinality, keyValues);
            } catch (ArangoDBVertex.CantAddValueToSinglePropertyException e) {
                throw new IllegalArgumentException("The property couldn't be created", e);
            }
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<Iterator<VertexProperty<V>>> iterators = new ArrayList<>();
        allPropertiesIfEmpty(propertyKeys).forEachRemaining(k -> iterators.add(properties.get(k).properties()));
        return Iterators.concat(iterators.iterator());
    }

    @SuppressWarnings("unchecked")
    private <V> VertexProperty<V> multivalue(
        String key,
        V value,
        VertexProperty.Cardinality cardinality,
        Object... keyValues) throws ArangoDBVertex.CantAddValueToSinglePropertyException {
        final ArngVertexProperty<V> result = new ArngVertexProperty<>(key, value, vertex);
        ElementHelper.attachProperties(result, keyValues);
        if (properties.containsKey(key) && properties.get(key).cardinality().equals(cardinality)) {
            properties.get(key).addValue(result);
        }
        else {
            Iterator<VertexProperty<V>> oldValues = properties(key);
            final VertexPropertyValue<V> propertyValue = new ArngVertexPropertyValue<>(cardinality, result);
            properties.put(key, propertyValue);
            propertyValue.addValues(oldValues);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> void removeProperty(VertexProperty<V> property) {
        assert property instanceof ArngVertexProperty;
        VertexPropertyValue value = properties.get(property.key());
        if (value.cardinality().equals(VertexProperty.Cardinality.single)) {
            properties.remove(property.key());
        }
        else {
            if (value.removeOne((ArngVertexProperty) property)) {
                properties.remove(property.key());
            }
        }
    }


    /**
     * Helper method for queries that want all elementProperties.
     * @param propertyKeys
     * @return
     */

    private Iterator<String> allPropertiesIfEmpty(String[] propertyKeys) {
        if (propertyKeys.length > 0) {
            return Arrays.asList(propertyKeys).iterator();
        }
        return keys().iterator();
    }

}
