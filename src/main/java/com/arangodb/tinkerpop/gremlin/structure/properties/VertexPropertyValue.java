package com.arangodb.tinkerpop.gremlin.structure.properties;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;

/**
 * This interface defines the API to access VertexProperties which can have single or multiple values
 *
 * @param <V>
 *
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public interface VertexPropertyValue<V> {

    /**
     * Get one VertexProperty for the given key. If the property's cardinality is {@link org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality#list}
     * or {@link org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality#set} and more than one value is
     * stored, an IllegalStateException is thrown.
     * @param key                   the property key
     * @return  a VertexProperty for the matching key
     * @throws IllegalStateException if the cardinality is set or list and more than one values are stored
     */
    VertexProperty<V> one(String key);

    /**
     * Get the values the properties as an {@link Iterator}.
     */
    Iterator<V> values();

    /**
     * Get an {@link Iterator} of properties.
     */
    Iterator<VertexProperty<V>> properties();

    /**
     * Get the cardinality of the property
     * @return
     */
    VertexProperty.Cardinality cardinality();

    /**
     * Add the given values to the existing ones. If the property's cardinality is {@link org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality#single}
     * a {@link com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex.CantAddValueToSinglePropertyException} exception
     * is thrown.
     *
     * @param values                an iterator with the values to add.
     * @throws ArangoDBVertex.CantAddValueToSinglePropertyException
     */
    void addValues(Iterator<VertexProperty<V>> values) throws ArangoDBVertex.CantAddValueToSinglePropertyException;

    /**
     * Add the given value to the existing ones. If the property's cardinality is {@link org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality#single}
     * a {@link com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex.CantAddValueToSinglePropertyException} exception
     * is thrown.
     *
     * @param value                 the value to add
     * @throws ArangoDBVertex.CantAddValueToSinglePropertyException
     */

    void addValue(VertexProperty value) throws ArangoDBVertex.CantAddValueToSinglePropertyException;

    /**
     * Remove the given value to the existing ones. If the property's cardinality is {@link org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality#single}
     * a {@link com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex.CantRemoveValueFromSinglePropertyException} exception
     * is thrown.
     *
     * @param value                 the value to remove
     * @throws ArangoDBVertex.CantRemoveValueFromSinglePropertyException
     */

    boolean removeOne(ArngVertexProperty<V> value) throws ArangoDBVertex.CantRemoveValueFromSinglePropertyException;
}
