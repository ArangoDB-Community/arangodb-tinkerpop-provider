//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.arangodb.tinkerpop.gremlin.client.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;


/**
 * The Class ArangoDBVertexProperty.
 *
 * @param <V> the type of the property value
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBVertexProperty<V> extends ArangoDBElementProperty<V> implements VertexProperty<V> {

	/** The Logger. */
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertexProperty.class);

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBVertexProperty() {
		super();
	}

	/**
	 * Instantiates a new arango DB vertex property.
	 *
	 * @param name the name
	 * @param value the value
	 * @param owner the owner
	 */
	
	public ArangoDBVertexProperty(String name, V value, ArangoDBBaseDocument owner) {
		super(name, value, owner, ArangoDBGraph.ELEMENT_PROPERTIES_COLLECTION);
	}

    /**
     * Instantiates a new Arango DB vertex property.
     *
     * @param key the id
     * @param name the name
     * @param value the value
     * @param owner the owner
     */
	
    public ArangoDBVertexProperty(String key, String name, V value, ArangoDBBaseDocument owner) {
        super(key, name, value, owner, ArangoDBGraph.ELEMENT_PROPERTIES_COLLECTION);
    }

	@Override
    public String toString() {
    	return StringFactory.propertyString(this);
    }

	@Override
	public Object id() {
		return _id();
	}
	
	@Override
    public String label() {
        return name;
    }

    @Override
    public Vertex element() {
        ArangoCursor<ArangoDBVertex> q = graph.getClient()
        		.getDocumentNeighbors(this, Collections.emptyList(), Direction.IN, ArangoDBPropertyFilter.empty(), ArangoDBVertex.class);
        ArangoDBIterator<ArangoDBVertex> iterator = new ArangoDBIterator<ArangoDBVertex>(graph, q);
        return iterator.hasNext() ? iterator.next() : null;
    }

	@Override
	public <U> Property<U> property(String key, U value) {
		logger.info("property {} = {}", key, value);
		ElementHelper.validateProperty(key, value);
        Property<U> p = property(key);
        if (!p.isPresent()) {
            p = ArangoDBUtil.createArangoDBPropertyProperty(key, value, this);
        } else {
            ((ArangoDBElementProperty<U>) p).value(value);
        }
        return p;
	}


	@SuppressWarnings("unchecked")
	@Override
	public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        List<String> labels = new ArrayList<>();
        labels.add(graph.getPrefixedCollectioName(ArangoDBGraph.ELEMENT_PROPERTIES_EDGE_COLLECTION));
        ArangoDBPropertyFilter filter = new ArangoDBPropertyFilter();
        for (String pk : propertyKeys) {
            filter.has("name", pk, ArangoDBPropertyFilter.Compare.EQUAL);
        }
        ArangoCursor<?> query = graph.getClient().getElementProperties(this, labels, filter, ArangoDBPropertyProperty.class);
        return new ArangoDBPropertyIterator<>(graph, (ArangoCursor<ArangoDBPropertyProperty<U>>) query);
	}

    @Override
    public void remove() {
        logger.info("remove {}", this._id());
        if (paired) {
            // Delete properties
            properties().forEachRemaining(Property::remove);
        }
        super.remove();
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