package com.arangodb.tinkerpop.gremlin.structure.properties;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.Set;

/**
 * This interface defines the API for Arango Verticesto delegate the property access.
 *
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public interface VertexProperties {

    <V> void removeProperty(VertexProperty<V> property);

    /**
     * @see Element#keys()
     */
    Set<String> keys();

    /**
     * @see Vertex#property(String)
     * @return
     */
    <V> VertexProperty<V> property(String key);

    /**
     * @see Vertex#property(String, Object)
     */
    <V> VertexProperty<V> property(String key, V value);

    /**
     * @see Element#values(String...)
     */
    <V> Iterator<V> values(String... propertyKeys);


    /**
     * @see Vertex#property(VertexProperty.Cardinality, String, Object, Object...)
     */
    <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues);

    /**
     * @see Vertex#properties(String...)
     */
    <V> Iterator<VertexProperty<V>> properties(String... propertyKeys);

}
