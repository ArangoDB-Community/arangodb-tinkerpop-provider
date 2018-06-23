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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.collections4.IterableMap;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;


/**
 * The ArangoDB vertex class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBVertex extends ArangoDBElement implements Vertex, ArangoDBDocument {
	
	public class ArangoDBVertexProperty<V> extends ArangoDBProperty<V> implements VertexProperty<V>, Entry<String, V>, Element {
		
		private final class ArangoProperty<PV> implements Property<PV> {

	        private final String key;
	        private final PV value;
	        private final ArangoDBVertexProperty<V> element;

	        private ArangoProperty(final String key, final PV value, final ArangoDBVertexProperty<V> element) {
	            this.key = key;
	            this.value = value;
	            this.element = element;
	        }

	        @Override
	        public String key() {
	            return this.key;
	        }

	        @Override
	        public PV value() throws NoSuchElementException {
	            return this.value;
	        }

	        @Override
	        public boolean isPresent() {
	            return true;
	        }

	        @Override
	        public Element element() {
	            return this.element;
	        }

	        @Override
	        public void remove() {
	            this.element.properties.remove(key);
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
		
		protected String label;
		
		protected IterableMap<String, ArangoProperty<?>> properties = new HashedMap<>(4, 0.75f);

		protected ArangoDBVertexProperty(HashEntry<String, V> next, int hashCode, Object key, V value,
				ArangoDBVertex element) {
			super(next, hashCode, key, value, element);
		}

		@Override
		public Object id() {
			return label;
		}

		@Override
		public <U> Property<U> property(String key, U value) {
			ElementHelper.validateProperty(key, value);
			ArangoProperty<U> property = new ArangoProperty<U>(key, value, this);
			this.properties.put(key, property);
			return property;
		}

		@Override
		public Vertex element() {
			return (Vertex) super.element();
		}

		@SuppressWarnings("unchecked")
		@Override
		public <U> Iterator<Property<U>> properties(String... propertyKeys) {
			return this.properties.entrySet().stream()
					.filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
					.map(entry -> entry.getValue())
					.map(v -> (Property<U>)v)
					.iterator();
		}
		
	}
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);
	
	public ArangoDBVertex() {
		super();
	}

	public ArangoDBVertex(ArangoDBGraph graph, String collection, String key) {
		super(graph, collection, key);
	}

	public ArangoDBVertex(ArangoDBGraph graph, String collection) {
		super(graph, collection);
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
    protected HashEntry<String, Object> createEntry(final HashEntry<String, Object> next, final int hashCode,
    		final String key, final Object value) {
    	
        return new ArangoDBVertexProperty<Object>(next, hashCode, convertKey(key), value, this);
    }
    
	@Override
	public void remove() {
		logger.info("removed {}", this._key());
		graph.getClient().deleteVertex(graph, this);
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
		logger.info("addEdge in collection {} to vertex {}", inVertex.id());
		ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
		if (!graph.edgeCollections().contains(label)) {
			throw new IllegalArgumentException("Edge label not in defined edge collections.");
		}
		ArangoDBEdge edge = new ArangoDBEdge();
		ElementHelper.attachProperties(edge, keyValues);
		graph.getClient().insertEdge(graph, edge);
		return edge;
	}


	@SuppressWarnings("unchecked")
	@Override
	public <V> VertexProperty<V> property(Cardinality cardinality, String key, V value, Object... keyValues) {
		logger.info("property {} = {} ({})", key, value, keyValues);
		ElementHelper.validateProperty(key, value);
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		ArangoDBVertexProperty<V> result;
		Object rawValues;
		switch(cardinality) {
		case single:
			put(key, value);
			break;
		case list:
			List<V> values;
			if (containsKey(key)) {
				rawValues = get(key);
				if (!(rawValues instanceof List<?>)) {
					throw new IllegalArgumentException("The current value does not match the desired cardinality");
				}
				values = (List<V>) rawValues; 
			}
			else {
				values = new ArrayList<>();
			}
			values.add(value);
			break;
		case set:
			Set<V> setValues;
			if (containsKey(key)) {
				rawValues = get(key);
				if (!(rawValues instanceof Set<?>)) {
					throw new IllegalArgumentException("The current value does not match the desired cardinality");
				}
				setValues = (Set<V>) rawValues; 
			}
			else {
				setValues = new HashSet<>();
			}
			setValues.add(value);
			break;
		}
		result = (ArangoDBVertexProperty<V>) getEntry(key);
		ElementHelper.attachProperties(result, keyValues);
		return result;
	}


	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
		
		ArangoDBQuery query = graph.getClient().getVertexEdges(graph, this, Arrays.asList(edgeLabels), direction);
		return query.getCursorResult(ArangoDBEdge.class);
	}


	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
		ArangoDBQuery query = graph.getClient().getVertexNeighbors(graph, this, Arrays.asList(edgeLabels), direction);
		return query.getCursorResult(ArangoDBVertex.class);	
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
		
		return this.entrySet().stream().map(e -> (VertexProperty<V>) e).iterator();
	}

	/**
	 * Creates a vertex
	 * 
	 * @param graph
	 *            a ArangoDBGraph
	 * @param id
	 *            the id (key) of the vertex (can be null)
	 * 
	 * @throws IllegalArgumentException
	 */
//	static ArangoDBVertex create(ArangoDBGraph graph, Object id) {
//		String key = (id != null) ? id.toString() : null;
//
//		try {
//			ArangoDBSimpleVertex v = graph.getClient().createVertex(graph.getRawGraph(), key, null);
//			return build(graph, v);
//		} catch (ArangoDBException e) {
//			if (e.errorNumber() == 1210) {
//				throw ExceptionFactory.vertexWithIdAlreadyExists(id);
//			}
//			logger.debug("could not create vertex", e);
//			throw new IllegalArgumentException(e.getMessage());
//		}
//	}

	/**
	 * Creates a vertex by loading it
	 * 
	 * @param graph
	 *            a ArangoDBGraph
	 * @param id
	 *            the id (key) of the vertex (can be null)
	 * 
	 * @throws IllegalArgumentException
	 */
//	static ArangoDBVertex load(ArangoDBGraph graph, Object id) {
//		if (id == null) {
//			throw ExceptionFactory.vertexIdCanNotBeNull();
//		}
//
//		String key = id.toString();
//
//		try {
//			ArangoDBSimpleVertex v = graph.getClient().getVertex(graph.getRawGraph(), key);
//			return build(graph, v);
//		} catch (ArangoDBException e) {
//			// nothing found
//			logger.debug("graph not found", e);
//			return null;
//		}
//	}

//	static ArangoDBVertex build(ArangoDBGraph graph, ArangoDBSimpleVertex simpleVertex) {
//		return new ArangoDBVertex(graph, simpleVertex);
//	}

//	@Override
//	public Iterable<Edge> getEdges(Direction direction, String... labels) {
//		if (document.isDeleted()) {
//			return null;
//		}
//		ArangoDBVertexQuery q = new ArangoDBVertexQuery(graph, this);
//		q.direction(direction);
//		q.labels(labels);
//
//		return q.edges();
//	}
//
//	@Override
//	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
//		if (document.isDeleted()) {
//			return null;
//		}
//		ArangoDBVertexQuery q = new ArangoDBVertexQuery(graph, this);
//		q.direction(direction);
//		q.labels(labels);
//
//		return q.vertices();
//	}
//
//	@Override
//	public VertexQuery query() {
//		if (document.isDeleted()) {
//			return null;
//		}
//		return new ArangoDBVertexQuery(graph, this);
//	}
//
//	/**
//	 * Returns the ArangoDBSimpleVertex
//	 * 
//	 * @return a ArangoDBSimpleVertex
//	 */
//
//	public ArangoDBSimpleVertex getRawVertex() {
//		return (ArangoDBSimpleVertex) document;
//	}
//
//	@Override
//	public String toString() {
//		return StringFactory.vertexString(this);
//	}
//
//	@Override
//	public void remove() {
//		if (document.isDeleted()) {
//			return;
//		}
//
//		try {
//			graph.getClient().deleteVertex(graph.getRawGraph(), (ArangoDBSimpleVertex) document);
//		} catch (ArangoDBException ex) {
//			// ignore error
//			logger.debug("could not delete vertex", ex);
//		}
//	}
//
//	@Override
//	public void save() throws ArangoDBException {
//		if (document.isDeleted()) {
//			return;
//		}
//		graph.getClient().saveVertex(graph.getRawGraph(), (ArangoDBSimpleVertex) document);
//	}
//
//	@Override
//	public Edge addEdge(String label, Vertex inVertex) {
//
//		if (label == null) {
//			throw ExceptionFactory.edgeLabelCanNotBeNull();
//		}
//
//		return ArangoDBEdge.create(this.graph, null, this, inVertex, label);
//	}

}

