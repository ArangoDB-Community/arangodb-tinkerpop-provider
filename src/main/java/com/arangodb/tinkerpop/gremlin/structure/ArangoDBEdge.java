//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseEdge;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph.ArangoDBIterator;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

/**
 * The ArangoDB Edge class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBEdge extends ArangoDBBaseEdge implements Edge {


	private static final Logger logger = LoggerFactory.getLogger(ArangoDBEdge.class);
	
	/** Tinkerpop ids are managed through keys, so we need to keep that information */
	
	private String from_key;

	/** Tinkerpop ids are managed through keys, so we need to keep that information */
	
	private String to_key;
	
    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBEdge() {
        super();

    }

    /**
     * Create a new ArangoDBEdge that connects the given vertices
     * @param graph         the graph in which the edge is created
     * @param collection    the collection into with the edge is created
     * @param from          the source vertex
     * @param to            the target vertex
     * @param key           the edge key
     */

	public ArangoDBEdge(
	    ArangoDBGraph graph,
        String collection,
        ArangoDBVertex from,
        ArangoDBVertex to,
        String key) {
		super(from._id(), to._id(), key, graph, collection);
        this.graph = graph;
        this.collection = collection;
		this.from_key = from._key();
		this.to_key = to._key();
	}

    /**
     * Create a new ArangoDBEdge that connects the given vertices
     * @param graph         the graph in which the edge is created
     * @param collection    the collection into with the edge is created
     * @param from          the source vertex
     * @param to            the target vertex
     */

	public ArangoDBEdge(
	    ArangoDBGraph graph,
        String collection,
        ArangoDBVertex from,
        ArangoDBVertex to) {
		this(graph, collection, from, to, null);
	}

    @Override
    public Object id() {
        return _key;
    }

    @Override
    public String label() {
        return collection();
    }


	@Override
	public <V> Property<V> property(String key, V value) {
		logger.info("property {} = {}", key, value);
		ElementHelper.validateProperty(key, value);
		Property<V> p = property(key);
		if (!p.isPresent()) {
            p = ArangoDBUtil.createArangoDBEdgeProperty(key, value, this);
        }
		else {
			((ArangoDBEdgeProperty<V>) p).value(value);
		}
		return p;
	}

	@Override
	public void remove() {
		logger.info("removing {} from graph {}.", this._key(), graph.name());
		graph.getClient().deleteEdge(graph.name(), this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Vertex> vertices(Direction direction) {
		List<String> ids = new ArrayList<>();
		switch(direction) {
		case BOTH:
			ids.add(from_key);
			ids.add(to_key);
			break;
		case IN:
			ids.add(from_key);
			break;
		case OUT:
			ids.add(to_key);
			break;
		}
		ArangoDBQuery query = graph.getClient().getGraphVertices(graph, ids);
		return new ArangoDBIterator<Vertex>(graph, query.getCursorResult(ArangoDBVertex.class));
	}
	
	/**
	 * Removing a property while iterating will throw ConcurrentModificationException 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        List<String> labels = new ArrayList<>();
        labels.add(ArangoDBUtil.ELEMENT_PROPERTIES_EDGE);
        ArangoDBPropertyFilter filter = new ArangoDBPropertyFilter();
        for (String pk : propertyKeys) {
            filter.has("key", pk, ArangoDBPropertyFilter.Compare.EQUAL);
        }
        ArangoDBQuery query = graph.getClient().getDocumentNeighbors(graph, this, labels, Direction.OUT, filter);
        return new ArangoDBIterator<Property<V>>(graph, query.getCursorResult(ArangoDBEdgeProperty.class));
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
