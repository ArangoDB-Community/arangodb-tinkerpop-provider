package com.arangodb.tinkerpop.gremlin.structure;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseEdge;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

/**
 * The Class ArangoDBProperty.
 *
 * @param <V> the value type
 */

public abstract class ArangoDBElementProperty<V> extends ArangoDBBaseDocument implements Property<V> {

    /**
     * An Edge to link an ArangoDBBaseDocument to one of its properties. The from paramter is an ArangoDBBaseDocument
     * since we allow ArangoDBElementProperty to have properties too.
     */
    public static class ElementHasProperty extends ArangoDBBaseEdge {

        public ElementHasProperty(ArangoDBBaseDocument from, ArangoDBElementProperty<?> to, ArangoDBGraph graph) {
            super(from._id(), to._id(), graph, ArangoDBUtil.ELEMENT_PROPERTIES_EDGE);
        }
    }
	
	protected String key;

    protected V value;


    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBElementProperty() { super();	}

    public ArangoDBElementProperty(String id, String key, V value, ArangoDBBaseDocument owner, String collection) {
	    this._key = id;
        this.key = key;
        this.value = value;
        this.graph = owner.graph();
        this.collection = collection;
    }

    public ArangoDBElementProperty(String key, V value, ArangoDBBaseDocument owner, String collection) {
        this(null, key, value, owner, collection);
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
		try {
			graph.getClient().deleteDocument(graph.name(), this);
		} catch (ArangoDBGraphException ex) {
			// Pass Removing a property that does not exists should not throw an exception.
		}
    }

	@SuppressWarnings("unchecked")
	@Override
	public V value() throws NoSuchElementException {
        return (V) ArangoDBUtil.getCorretctPrimitive(value);
	}
	
	public V value(V value) {
		V oldValue = this.value;
		this.value = value;
        if (!value.equals(oldValue)) {
            save();
        }
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

    /**
     * Assign this property to a Document and create an ElementHasProperty that represents the connection.
     * @param doc The document
     * @return an ElementHasProperty (edge) that connects the element to this property
     */
    public ElementHasProperty assignToElement(ArangoDBBaseDocument doc) {
        return new ElementHasProperty(doc, this, doc.graph());
    }

    public void save() {
        if (paired) {
            graph.getClient().updateDocument(graph.name(), this);
        }
    }
}