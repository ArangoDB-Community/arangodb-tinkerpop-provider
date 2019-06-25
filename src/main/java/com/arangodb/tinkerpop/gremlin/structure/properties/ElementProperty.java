package com.arangodb.tinkerpop.gremlin.structure.properties;

import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Iterator;

/**
 * Additional methods for ElementProperties
 * @param <V>                       the type of the property baseValue
 *
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public interface ElementProperty<V> extends Property<V> {

    /**
     * Return an {@link Iterator} of this property's baseValue
     * @return  iterator of values
     */
    Iterator<V> values();
}
