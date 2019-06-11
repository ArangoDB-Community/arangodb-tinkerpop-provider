//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.velocypack.annotations.Expose;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;


/**
 * The Class ArangoDBProperty.
 *
 * @param <V> the property value type
 *
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBElementProperty<V> implements Property<V> {

    /** The key (name) that identifies this property. */

    protected String key;

    /** The element that owns the property */
    @Expose(serialize = false, deserialize = false)
    protected ArangoDBElement element;

    /** The value of the property */

    protected V value;

    /** The canonical name of the value's Java type */

    protected String type;

    /** Empty constructor for de-/serialization */
    public ArangoDBElementProperty() { }


    public ArangoDBElementProperty(String key, V value, ArangoDBElement element) {
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

    public void element(ArangoDBElement element) {
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

}