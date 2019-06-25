//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure.properties;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.ArngElement;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.Set;


/**
 * An implementation of {@link VertexProperty} for ArangoDB vertices. This implementation delegates property access to
 * an {@link ElementProperties} instance.
 *
 * @param <V> the type of the property value
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArngVertexProperty<V> extends ArngElementProperty<V> implements VertexProperty<V>, ArngElement {

    /** All property access is delegated */

    private final ElementProperties properties;

    /**
     * Instantiates a new arango DB vertex property using an {@link ArngElementProperties} as delegate for
     * property acess.
     *
     * @param name                  the property name
     * @param value                 the property value
     * @param owner                 the property owner
     */

    public ArngVertexProperty(
            String name,
            V value,
            ArangoDBVertex owner) {
        this(name, value, owner, new ArngElementProperties());
    }
	/**
	 * Instantiates a new arango DB vertex property.
	 *
	 * @param name                  the property name
	 * @param value                 the property value
	 * @param owner                 the property owner
     * @param properties            the delegate responsible of property access
	 */
	
	public ArngVertexProperty(
        String name,
        V value,
        ArangoDBVertex owner,
        ElementProperties properties) {
		super(name, value, owner);
        this.properties = properties;
	}

	@Override
	public Object id() {
		return key;
	}
	
	@Override
    public String label() {
        return key;
    }

    @Override
    public Vertex element() {
        return (Vertex) element;
    }
    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return properties.properties(propertyKeys);
    }

    @Override
    public Set<String> keys() {
        return properties.keys();
    }

    @Override
    public <U> Property<U> property(String key) {
        return properties.property(key);
    }

    @Override
    public <U> Iterator<U> values(String... propertyKeys) {
        return properties.values(propertyKeys);
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        final Property<U> property = properties.property(key, value, this);
        update();
        return property;
    }

    @Override
    public void removeProperty(Property<?> property) {
        properties.removeProperty(property.key());
    }

    @Override
    public void update() {
        element.update();
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
    	return key().hashCode() + value().hashCode();
    }

}