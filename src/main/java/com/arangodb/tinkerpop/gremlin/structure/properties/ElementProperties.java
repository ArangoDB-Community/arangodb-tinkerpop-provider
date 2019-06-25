package com.arangodb.tinkerpop.gremlin.structure.properties;

import com.arangodb.tinkerpop.gremlin.structure.ArngElement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Iterator;
import java.util.Set;

/**
 * This interface defines the API for Arango Edges and VertexProperties to delegate the property access.
 *
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public interface ElementProperties {

    /**
     * Remove a property from the element
     * @param key                   The key of property to remove
     */

    void removeProperty(String key);

//    /**
//     * Add all elementProperties to the element. This method is used for deserialization.
//     *
//     * @param elementProperties            An iterator of the elementProperties to add
//     */
//    <V> void attachProperties(Iterator<Property<V>> elementProperties);

    // Copy of Element API related to properties

    /**
     * @see Element#keys()
     */
    Set<String> keys();

    /**
     * @see Element#property(String)
     */
    <V> Property<V> property(String key);

    /**
     * @see Element#property(String, Object)
     * @param element               the element that owns the property
     */
    <V> Property<V> property(String key, V value, ArngElement element);

    /**
     * @see Element#values(String...)
     */
    <V> Iterator<V> values(String... propertyKeys);

    /**
     * @see Element#properties(String...)
     */
    <V> Iterator<Property<V>> properties(String... propertyKeys);
}
