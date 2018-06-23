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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.entity.DocumentField;
import com.arangodb.entity.DocumentField.Type;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;

/**
 * The ArangoDB edge class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBEdge extends ArangoDBElement implements Edge, ArangoDBDocument {


	private static final Logger logger = LoggerFactory.getLogger(ArangoDBEdge.class);

	@DocumentField(Type.FROM)
	private String _arango_from;

	@DocumentField(Type.TO)
	private String _arango_to;
	
	/**
     * Creates an entry to store the key-value data.
     *
     * @param next  the next entry in sequence
     * @param hashCode  the hash code to use
     * @param key  the key to store
     * @param value  the value to store
     * @return the newly created entry
     */
    protected HashEntry<String, Object> createEntry(final HashEntry<String, Object> next, final int hashCode,
    		final String key, final Object value) {
    	
        return new ArangoDBProperty<Object>(next, hashCode, convertKey(key), value, this);
    }

	@SuppressWarnings("unchecked")
	@Override
	public <V> Property<V> property(String key, V value) {
		logger.info("property {} = {}", key, value);
		ElementHelper.validateProperty(key, value);
		put(key, value);
		return (ArangoDBProperty<V>) getEntry(key);
	}
	
	
	@Override
	public void remove() {
		logger.info("removed {}", this._key());
		graph.getClient().deleteEdge(graph, this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Vertex> vertices(Direction direction) {
		List<String> ids = new ArrayList<>();
		switch(direction) {
		case BOTH:
			ids.add(_arango_from);
			ids.add(_arango_to);
			break;
		case IN:
			ids.add(_arango_from);
			break;
		case OUT:
			ids.add(_arango_to);
			break;
		}
		ArangoDBQuery query = graph.getClient().getGraphVertices(graph, ids);
		return query.getCursorResult(ArangoDBVertex.class);	
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<Property<V>> properties(String... propertyKeys) {
		
		return this.entrySet().stream().map(e -> (Property<V>) e).iterator();
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
