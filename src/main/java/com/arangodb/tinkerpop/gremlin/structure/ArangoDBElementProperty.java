package com.arangodb.tinkerpop.gremlin.structure;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * The Class ArangoDBProperty.
 *
 * @param <U> the value type
 */

public class ArangoDBElementProperty<V> implements Property<V> {
	
	private String key;
	
	private V value;

	private ArangoDBElement owner; 
	
	protected ArangoDBElementProperty(String key, V value, ArangoDBElement element) {
		this.key = key;
		this.value = value;
		this.owner = element;
	}

	@Override
	public Element element() {
		return owner;
	}

	@Override
	public boolean isPresent() {
		return value != null;
	}

	@Override
	public String key() {
		return key;
	}

	@Override
	public void remove() {
		owner.removeProperty(this);
	}

	@Override
	public V value() throws NoSuchElementException {
		return value;
	}
	
	public V value(V value) {
		V oldValue = this.value;
		this.value = value;
		return oldValue;
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
        return ElementHelper.hashCode(this);
    }
	
}