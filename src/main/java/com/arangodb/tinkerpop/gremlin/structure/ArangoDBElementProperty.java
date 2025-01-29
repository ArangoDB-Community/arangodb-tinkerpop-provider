//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.NoSuchElementException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseEdge;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Class ArangoDBProperty.
 *
 * @param <V> the property value type
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public abstract class ArangoDBElementProperty<V> extends ArangoDBBaseDocument implements Property<V> {

    private static final Logger logger = LoggerFactory.getLogger(ArangoDBElementProperty.class);

    /**
     * An Edge to link an ArangoDBBaseDocument to one of its properties. The from parameter is an
     * ArangoDBBaseDocument since we allow ArangoDBElementProperty to have properties too.
     */
	
    public static class ElementHasProperty extends ArangoDBBaseEdge {

        /**
         * Instantiates a new element has property.
         *
         * @param from the from
         * @param to the to
         * @param graph the graph
         */
        public ElementHasProperty(ArangoDBBaseDocument from, ArangoDBElementProperty<?> to, ArangoDBGraph graph) {
            super(ArangoDBGraph.ELEMENT_PROPERTIES_EDGE_COLLECTION, from._id(), to._id(), graph);
        }
    }
	
	/** The property name. */

    @JsonProperty
	protected String name;

    /** The property value. */

    @JsonProperty
    protected V value;
    
    /** The property type */

    @JsonProperty
    protected String valueType;


    /**
     * Constructor used for Arango DB JavaBeans serialisation.
     */

	public ArangoDBElementProperty() { super();	}

    /**
     * Instantiates a new Arango DB element property.
     *
     * @param key the id
     * @param name the name
     * @param value the value
     * @param owner the owner
     * @param label the label
     */
	
    public ArangoDBElementProperty(String key, String name, V value, ArangoDBBaseDocument owner, String label) {
	    super(key, label, owner.graph());
        this.name = name;
        this.value = value;
        this.valueType = (value != null ? value.getClass() : Void.class).getCanonicalName();
    }

    /**
     * Instantiates a new Arango DB element property.
     *
     * @param name the name
     * @param value the value
     * @param owner the owner
     * @param label the collection
     */
    
    public ArangoDBElementProperty(String name, V value, ArangoDBBaseDocument owner, String label) {
        this(null, name, value, owner, label);
    }

    @JsonIgnore
	@Override
	public boolean isPresent() {
		return value != null;
	}

	@Override
	public String key() {
		return name;
	}

	@Override
	public void remove() {
        logger.info("remove {}", this._id());
        if (paired) {
            //Remove vertex
            try {
                graph.getClient().deleteDocument(this);
            } catch (ArangoDBGraphException ex) {
                // Pass Removing a property that does not exists should not throw an exception.
            }
        }
    }

	@SuppressWarnings("unchecked")
	@Override
	public V value() throws NoSuchElementException {
        return (V) ArangoDBUtil.getCorretctPrimitive(value, valueType);
	}
	
	/**
	 * Value.
	 *
	 * @param value the value
	 * @return the v
	 */
	public V value(V value) {
		V oldValue = this.value;
		this.value = value;
        if (!value.equals(oldValue)) {
            save();
        }
		return oldValue;
    }

    /**
     * Save.
     */

    public void save() {
        if (paired) {
            graph.getClient().updateDocument(this);
        }
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
     * @param doc                    The document
     * @return an ElementHasProperty (edge) that connects the element to this property
     */
	
    public ElementHasProperty assignToElement(ArangoDBBaseDocument doc) {
        return new ElementHasProperty(doc, this, doc.graph());
    }
}