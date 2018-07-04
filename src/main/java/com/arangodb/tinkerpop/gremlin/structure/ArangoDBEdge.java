//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph.ArangoDBIterator;

/**
 * The ArangoDB edge class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBEdge<T> extends ArangoDBElement<T> implements Edge { //, ArangoDBDocument {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBEdge.class);

	public static final String FROM_KEY = "arangodb_from_key";
	public static final String TO_KEY = "arangodb_to_key";

	private String arango_db_from;

	private String arango_db_to;
	
	/** Tinkerpop ids are managed through keys, so we need to keep that information */
	private String from_key;

	/** Tinkerpop ids are managed through keys, so we need to keep that information */
	private String to_key;
	
	public ArangoDBEdge() {
		super();
	}

	public ArangoDBEdge(ArangoDBGraph graph, String collection, String key, ArangoDBVertex<?> from, ArangoDBVertex<?> to) {
		super(graph, collection, key);
		this.arango_db_from = from._id();
		this.arango_db_to = to._id();
		this.from_key = from._key();
		this.to_key = to._key();
	}

	public ArangoDBEdge(ArangoDBGraph graph, String collection, ArangoDBVertex<?> from, ArangoDBVertex<?> to) {
		this(graph, collection, null, from, to);
	}

	/**
     * Creates an entry to store the key-value data.
     *
     * @param next  the next entry in sequence
     * @param hashCode  the hash code to use
     * @param key  the key to store
     * @param value  the value to store
     * @return the newly created entry
     */
    protected HashEntry<String, T> createEntry(
    	final HashEntry<String, T> next,
    	final int hashCode,
    	final String key,
    	final T value) {
        return new ArangoDBProperty<T>(next, hashCode, convertKey(key), value, this);
    }

	@SuppressWarnings("unchecked")
	@Override
	public <PV> Property<PV> property(String key, PV value) {
		logger.info("property {} = {}", key, value);
		ElementHelper.validateProperty(key, value);
		Object oldValue = put(key, (T) value);
		if (paired && !value.equals(oldValue)) {
			try {
				graph.getClient().updateEdge(graph, this);
			} catch (ArangoDBGraphException e) {
				logger.error("Unable to update edge in DB", e);
				throw new IllegalStateException("Unable to update edge in DB", e);
			}
		}
		return (ArangoDBProperty<PV>) getEntry(key);
	}
	
	
	@Override
	public void remove() {
		logger.info("removed {}", this._key());
		try {
			graph.getClient().deleteEdge(graph, this);
		} catch (ArangoDBGraphException e) {
			logger.error("Unable to remove edge in DB", e);
			throw new IllegalStateException("Unable to remove edge in DB", e);
		}
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
			ids.add(to_key);
			break;
		case OUT:
			ids.add(from_key);
			break;
		}
		ArangoDBQuery query = graph.getClient().getGraphVertices(graph, ids);
		try {
			return new ArangoDBIterator<Vertex>(graph, query.getCursorResult(ArangoDBVertex.class));
		} catch (ArangoDBGraphException e) {
			return null;
		}	
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<Property<V>> properties(String... propertyKeys) {
		List<String> validProperties = getValidProperties(propertyKeys);
		return new ArangoElementPropertyIterator<Property<V>, V>((ArangoDBElement<V>) this, validProperties);
	}

	

	@SuppressWarnings("unchecked")
	@Override
	public <V> Property<V> property(String key) {
		return (Property<V>) entrySet().stream().filter(e -> e.getKey().equals(key)).findFirst().get();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> V value(String key) throws NoSuchElementException {
		return (V) get(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<V> values(String... propertyKeys) {
		// FIXME Is this a filtering operation too?
		return (Iterator<V>) Arrays.stream(propertyKeys).map(this::get).iterator();
	}

	public String _from() {
		return arango_db_from;
	}

	public void _from(String from) {
		this.arango_db_from = from;
	}

	public String _to() {
		return arango_db_to;
	}

	public void _to(String to) {
		this.arango_db_to = to;
	}
	
	public String from_key() {
		return from_key;
	}

	public void from_key(String from_key) {
		this.from_key = from_key;
	}

	public String to_key() {
		return to_key;
	}

	public void to_key(String to_key) {
		this.to_key = to_key;
	}

	@Override
    public String toString() {
    	return StringFactory.edgeString(this);
    }
	
	
	
	
	
//	private ArangoDBEdge(ArangoDBGraph graph, ArangoDBSimpleEdge edge, Vertex outVertex, Vertex inVertex) {
//		this.graph = graph;
//		this.document = edge;
//		this.outVertex = outVertex;
//		this.inVertex = inVertex;
//	}

//	static ArangoDBEdge create(ArangoDBGraph graph, Object id, Vertex outVertex, Vertex inVertex, String label) {
//		String key = (id != null) ? id.toString() : null;
//
//		if (outVertex instanceof ArangoDBVertex && inVertex instanceof ArangoDBVertex) {
//			ArangoDBVertex from = (ArangoDBVertex) outVertex;
//			ArangoDBVertex to = (ArangoDBVertex) inVertex;
//
//			try {
//				ArangoDBSimpleEdge v = graph.getClient().createEdge(graph.getRawGraph(), key, label,
//					from.getRawVertex(), to.getRawVertex(), null);
//				return build(graph, v, outVertex, inVertex);
//			} catch (ArangoDBException e) {
//				if (e.errorNumber() == 1210) {
//					throw ExceptionFactory.vertexWithIdAlreadyExists(id);
//				}
//
//				logger.debug("error while creating an edge", e);
//
//				throw new IllegalArgumentException(e.getMessage());
//			}
//		}
//		throw new IllegalArgumentException("Wrong vertex class.");
//
//	}
//
//	static ArangoDBEdge load(ArangoDBGraph graph, Object id) {
//		if (id == null) {
//			throw ExceptionFactory.edgeIdCanNotBeNull();
//		}
//
//		String key = id.toString();
//
//		try {
//			ArangoDBSimpleEdge v = graph.getClient().getEdge(graph.getRawGraph(), key);
//			return build(graph, v, null, null);
//		} catch (ArangoDBException e) {
//			// do nothing
//			logger.debug("error while reading an edge", e);
//
//			return null;
//		}
//	}
//
//	static ArangoDBEdge build(ArangoDBGraph graph, ArangoDBSimpleEdge simpleEdge, Vertex outVertex, Vertex inVertex) {
//		return new ArangoDBEdge(graph, simpleEdge, outVertex, inVertex);
//	}
//
//	@Override
//	public Vertex getVertex(Direction direction) throws IllegalArgumentException {
//		if (direction.equals(Direction.IN)) {
//			if (inVertex == null) {
//				Object id = document.getProperty(ArangoDBSimpleEdge._TO);
//				inVertex = graph.getVertex(getKey(id));
//			}
//			return inVertex;
//		} else if (direction.equals(Direction.OUT)) {
//			if (outVertex == null) {
//				Object id = document.getProperty(ArangoDBSimpleEdge._FROM);
//				outVertex = graph.getVertex(getKey(id));
//			}
//			return outVertex;
//		} else {
//			throw ExceptionFactory.bothIsNotSupported();
//		}
//	}
//
//	private String getKey(Object id) {
//		if (id == null) {
//			return "";
//		}
//
//		String[] parts = id.toString().split("/");
//
//		if (parts.length > 1) {
//			return parts[1];
//		}
//
//		return parts[0];
//	}
//
//	@Override
//	public String getLabel() {
//		Object l = document.getProperty(StringFactory.LABEL);
//		if (l != null) {
//			return l.toString();
//		}
//
//		return null;
//	}
//
//	/**
//	 * Returns the arangodb raw edge
//	 * 
//	 * @return a ArangoDBSimpleEdge
//	 */
//	public ArangoDBSimpleEdge getRawEdge() {
//		return (ArangoDBSimpleEdge) document;
//	}
//
//	@Override
//	public String toString() {
//		return StringFactory.edgeString(this);
//	}
//
//	@Override
//	public void remove() {
//		if (document.isDeleted()) {
//			return;
//		}
//		try {
//			graph.getClient().deleteEdge(graph.getRawGraph(), (ArangoDBSimpleEdge) document);
//		} catch (ArangoDBException ex) {
//			// ignore error
//			logger.debug("error while deleting an edge", ex);
//		}
//	}
//
//	@Override
//	public void save() throws ArangoDBException {
//		if (document.isDeleted()) {
//			return;
//		}
//		graph.getClient().saveEdge(graph.getRawGraph(), (ArangoDBSimpleEdge) document);
//	}

}
