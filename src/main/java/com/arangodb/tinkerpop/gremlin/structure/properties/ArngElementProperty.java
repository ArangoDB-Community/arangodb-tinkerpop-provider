//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure.properties;

import com.arangodb.tinkerpop.gremlin.structure.ArngElement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Implementation of {@link Property} for ArangoDB elements (Edges and VertexProperties)
 *
 * @param <V> the property value type
 *
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArngElementProperty<V> implements ElementProperty<V> {

    /** The primaryKey (name) that identifies this property. */

    protected String key;

    /** The element that owns the property */

    protected ArngElement element;

    /** The value of the property */

    protected V value;

    /** The canonical name of the value's Java type */

    protected String type;


    public ArngElementProperty(String key, V value, ArngElement element) {
        this.key = key;
        this.value = value;
        this.element = element;
        this.type = value == null ? null : value.getClass().getCanonicalName();
    }

    @Override
    public boolean isPresent() {
        return value != null;
    }

    @Override
    public Element element() {
        return element;
    }

    public void element(ArngElement element) {
        this.element = element;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public void remove() {
        element.removeProperty(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public V value() throws NoSuchElementException {
        if (this.value == null) {
            throw new NoSuchElementException("Property is empty.");
        }
        return value;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return key.hashCode() + value.hashCode();
    }


    @Override
    public Iterator<V> values() {
        return Collections.singletonList(value).iterator();
    }
}