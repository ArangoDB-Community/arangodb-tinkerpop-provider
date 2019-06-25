package com.arangodb.tinkerpop.gremlin.structure.properties;

import com.arangodb.tinkerpop.gremlin.structure.*;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

/**
 * An implementation of {@link ElementProperties} that uses a {@link HashMap} to store baseValue information for each key.
 * The stored values are instances of {@link ElementProperty}.
 *
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public class ArngElementProperties implements ElementProperties {

    /** The baseValue(s) of the elementProperties, keyd by property primaryKey (name) */

    protected final Map<String, ElementProperty> elementProperties = new HashMap<>();

    @Override
    public Set<String> keys() {
        return Collections.unmodifiableSet(elementProperties.keySet());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Property<V> property(final String key) {
        if (!elementProperties.containsKey(key)) {
            return Property.empty();
        }
        return elementProperties.get(key);
    }

    @Override
    public <V> Property<V> property(
            ArngElement element,
            final String key,
            final V value) {
        final ElementProperty<V> property = new ArngElementProperty<>(key, value, element);
        elementProperties.put(key, property);
        return property;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<V> values(String... propertyKeys) {
        List<Iterator<V>> iterators = new ArrayList<>();
        allPropertiesIfEmpty(propertyKeys).forEachRemaining(k -> iterators.add(elementProperties.get(k).values()));
        return Iterators.concat(iterators.iterator());
    }


    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        List<Property<V>> properties = new ArrayList<>();
        for (String key : propertyKeys) {
            properties.add(elementProperties.get(key));
        }
        return properties.iterator();
    }

//    /**
//     * Add a label of Properties that the manager needs to manage. This methods is used during de-serialization
//     * to add elementProperties to an Edge or VertexProperty
//     * @param elementProperties            The label of elementProperties.
//     */
//    @Override
//    public void attachVertexProperties(
//            String key,
//            Collection<ArngVertexProperty> elementProperties) {
//        this.elementProperties.put(key, elementProperties.stream().map(p -> (ArngElementProperty)p).collect(Collectors.toList()));
//        // this.cardinalities.put(key, elementProperties.stream().findFirst().map(ArngVertexProperty::getCardinality).orElse(VertexProperty.Cardinality.single));
//    }


    @Override
    public final void removeProperty(String key) {
        elementProperties.remove(key);
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
