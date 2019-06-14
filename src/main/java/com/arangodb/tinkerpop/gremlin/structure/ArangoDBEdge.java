//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;

import com.arangodb.tinkerpop.gremlin.client.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ArangoDB Edge class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBEdge extends ArangoDBBaseEdge implements Edge, ArangoDBElement {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBEdge.class);

	/** All property access is delegated to the property manager */

	protected ArangoDBPropertyManager pManager;

    /**
     * Constructor used for ArabgoDB JavaBeans de-/serialisation.
     */

	public ArangoDBEdge() {
		super();
		pManager = new ArangoDBPropertyManager(this);
	}

	/**
	 * Create a new ArangoDBEdge that connects the given vertices.
	 *  @param collection    the collection into with the edge is created
	 * @param from          the source vertex
	 * @param to            the target vertex
	 * @param graph         the graph in which the edge is created
	 */

	public ArangoDBEdge(
		String collection,
		ArangoDBVertex from,
		ArangoDBVertex to,
		ArangoDBGraph graph) {
		this(null, collection, from, to, graph);
	}


	/**
	 * Create a new ArangoDBEdge that connects the given vertices.
	 *
	 * @param key           the edge key
	 * @param collection    the collection into with the edge is created
	 * @param from          the source vertex
	 * @param to            the target vertex
	 * @param graph         the graph in which the edge is created
	 */

	public ArangoDBEdge(
		String key,
		String collection,
		ArangoDBVertex from,
		ArangoDBVertex to,
		ArangoDBGraph graph) {
		super(key, collection, from._id(), to._id(), graph);
		this.graph = graph;
		this.collection = collection;
		pManager = new ArangoDBPropertyManager(this);
	}

    @Override
    public Object id() {
        return _id();
    }

    @Override
    public String label() {
        return label;
    }

	@Override
	public void remove() {
		logger.info("removing {} from graph {}.", this._key(), graph.name());
		if (paired) {
			try {
				graph.getDatabaseClient().deleteEdge(this);
			} catch (ArangoDBGraphException ex) {

			}
		}
	}

	@Override
	public Iterator<Vertex> vertices(Direction direction) {
		boolean from = true;
		boolean to = true;
		switch(direction) {
		case BOTH:
			break;
		case IN:
			from = false;
			break;
		case OUT:
			to = false;
			break;
		}
		String edgeCollection = isPaired() ? label() : graph.getPrefixedCollectioName(label());
		return new ArangoDBIterator<>(graph, graph.getDatabaseClient().getEdgeVertices(_id(), edgeCollection, from, to));
	}

	@Override
	public void removeProperty(ArangoDBElementProperty<?> property) {
		pManager.removeProperty(property);
	}

	@Override
	public <V> Property<V> property(final String key) {
		logger.debug("Get property {}", key);
		return pManager.property(key);
	}

	@Override
	public <V> Iterator<Property<V>> properties(String... propertyKeys) {
		logger.debug("Get Properties {}", (Object[])propertyKeys);
		return pManager.properties(propertyKeys);
	}

	@Override
	public <V> Iterator<V> values(String... propertyKeys) {
		logger.debug("Get Values {}", (Object[])propertyKeys);
		return pManager.values(propertyKeys);
	}

	@Override
	public <V> Property<V> property(final String key, final V value) {
		return pManager.property(key, value);
	}

	@Override
	public void save() {
		if (paired) {
			graph.getDatabaseClient().updateEdge(this);
		}
	}

	@Override
	public Set<String> keys() {
		return pManager.keys();
	}

	/**
	 * This method is intended for rapid deserialization
	 * @return
	 */
	public void attachProperties(Collection<ArangoDBElementProperty> properties) {
		this.pManager.attachProperties(properties);
	}

	@Override
    public String toString() {
    	return StringFactory.edgeString(this);
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
