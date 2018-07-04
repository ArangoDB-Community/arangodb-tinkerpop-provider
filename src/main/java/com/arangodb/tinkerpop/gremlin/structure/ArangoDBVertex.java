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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.collections4.IterableMap;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDBException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph.ArangoDBIterator;


/**
 * The ArangoDB vertex class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBVertex<U> extends ArangoDBElement<U> implements Vertex {
	
	public static class ArangoDBVertexProperty<V> extends ArangoDBProperty<V> implements VertexProperty<V>, Entry<String, V>, Element {
		
		private final class ArangoProperty<PV> implements Property<PV> {

	        private final String key;
	        private final PV value;
	        private final ArangoDBVertexProperty<V> element;

	        private ArangoProperty(final String key, final PV value, final ArangoDBVertexProperty<V> element) {
	        	super();
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
		
		
		protected IterableMap<String, ArangoProperty<?>> properties = new HashedMap<>(4, 0.75f);


		protected ArangoDBVertexProperty(
			HashEntry<String, V> next,
			int hashCode,
			Object key,
			V value,
			ArangoDBVertex<V> element) {
			super(next, hashCode, key, value, element);
		}
		
		public ArangoDBVertexProperty(
			Object key,
			V value,
			ArangoDBVertex<V> element) {
			super(key, value, element);
		}

		@Override
		public Object id() {
			return key;
		}

		@Override
		public <VP> Property<VP> property(String key, VP value) {
			ElementHelper.validateProperty(key, value);
			if (key == T.id.name()) {
				if (!element().graph().features().vertex().properties().willAllowId(key)) {
					VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
				}
			}
			ArangoProperty<VP> property = new ArangoProperty<VP>(key, value, this);
			this.properties.put(key, property);
			return property;
		}

		@Override
		public Vertex element() {
			return (Vertex) super.element();
		}

		@SuppressWarnings("unchecked")
		@Override
		public <VP> Iterator<Property<VP>> properties(String... propertyKeys) {
			return this.properties.entrySet().stream()
					.filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
					.map(entry -> entry.getValue())
					.map(v -> (Property<VP>)v)
					.iterator();
		}

		@Override
		public boolean equals(Object obj) {
			return ElementHelper.areEqual(this, obj);
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
    protected HashEntry<String, U> createEntry(
    	final HashEntry<String, U> next,
    	final int hashCode,
    	final String key,
    	final U value) {
    	
        return new ArangoDBVertexProperty<U>(next, hashCode, convertKey(key), value, this);
    }
    
	@Override
	public void remove() {
		logger.info("remove {}", this._key());
		try {
			graph.getClient().deleteVertex(graph, this);
		} catch (ArangoDBGraphException e) {
			logger.error("Unable to remove vertex in DB", e);
			throw new IllegalStateException("Unable to remove edge in DB", e);
		}
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
		logger.info("addEdge in collection {} to vertex {}", label, inVertex == null ? "?" :inVertex.id());
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		ElementHelper.validateLabel(label);
		if (!graph.edgeCollections().contains(label)) {
			throw new IllegalArgumentException(String.format("Edge label (%s)not in graph edge collections.", label));
		}
		if (inVertex == null) {
			Graph.Exceptions.argumentCanNotBeNull("vertex");
		}
		Object id;
		ArangoDBEdge<Object> edge = null;
		if (ElementHelper.getIdValue(keyValues).isPresent()) {
        	id = ElementHelper.getIdValue(keyValues).get();
        	if (graph.features().edge().willAllowId(id)) {
        		edge = new ArangoDBEdge<Object>(graph, label, id.toString(), this, ((ArangoDBVertex<?>) inVertex));
        	}
        	else {
        		throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        	}
        }
		else {
			edge = new ArangoDBEdge<Object>(graph, label, this, ((ArangoDBVertex<?>) inVertex));
		}
		ElementHelper.attachProperties(edge, keyValues);
		try {
			graph.getClient().insertEdge(graph, edge);
		} catch (ArangoDBGraphException e) {
			throw ArangoDBGraph.Exceptions.getArangoDBException((ArangoDBException) e.getCause());
		}
		return edge;
	}


	@SuppressWarnings("unchecked")
	@Override
	public <V> VertexProperty<V> property(
		Cardinality cardinality,
		String key,
		V value,
		Object... keyValues) {
		logger.debug("property {} = {} ({})", key, value, keyValues);
		ElementHelper.validateProperty(key, value);
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		Optional<Object> id = ElementHelper.getIdValue(keyValues);
		if (id.isPresent()) {
			if (!graph.features().vertex().willAllowId(id.get())) {
				VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
			}
		}
		ArangoDBVertexProperty<V> result;
		boolean added = false;
		switch(cardinality) {
		case single:
			Object oldValue = put(key, (U) value);
			added = !value.equals(oldValue);
			break;
		case list:
			if (containsKey(key)) {
				List<V> values;
				oldValue = get(key);
				if (!(oldValue instanceof List<?>)) {
					values = new ArrayList<>();
					values.add((V) oldValue);
					put(key, (U) values);
				}
				else {
					values = (List<V>) oldValue;
				}
				added = values.add(value);
			}
			else {
				put(key, (U) value);
				added = true;
			}
			break;
		case set:
			if (containsKey(key)) {
				Set<V> setValues;
				oldValue = get(key);
				if (!(oldValue instanceof Set<?>)) {
					setValues = new HashSet<>();
					setValues.add((V) oldValue);
					put(key, (U) setValues);
				}
				else {
					setValues = (Set<V>) oldValue;
				}
				added = setValues.add(value);
			}
			else {
				put(key, (U) value);
				added = true;
			}
			break;
		}
		if (paired && added) {
			try {
				graph.getClient().updateVertex(graph, this);
			} catch (ArangoDBGraphException e) {
				logger.error("Unable to update vertex in DB", e);
				throw new IllegalStateException("Unable to update vertex in DB", e);
			}
		}
		result = (ArangoDBVertexProperty<V>) getEntry(key);
		ElementHelper.attachProperties(result, keyValues);
		result.cardinality(cardinality);
		return result;
	}


	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
		
		ArangoDBQuery query = graph.getClient().getVertexEdges(graph, this, Arrays.asList(edgeLabels), direction);
		try {
			return new ArangoDBIterator<Edge>(graph, query.getCursorResult(ArangoDBEdge.class));
		} catch (ArangoDBGraphException e) {
			// TODO Auto-generated catch block
			return null;
		}
	}


	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
		ArangoDBQuery query = graph.getClient().getVertexNeighbors(graph, this, Arrays.asList(edgeLabels), direction);
		try {
			return new ArangoDBIterator<Vertex>(graph, query.getCursorResult(ArangoDBVertex.class));
		} catch (ArangoDBGraphException e) {
			return Collections.emptyIterator();
		}	
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
		List<String> validProperties = getValidProperties(propertyKeys);
		return new ArangoElementPropertyIterator<VertexProperty<V>, V>((ArangoDBElement<V>) this, validProperties);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> VertexProperty<V> property(String key) {
		return (VertexProperty<V>) entrySet().stream().filter(e -> e.getKey().equals(key)).findFirst().get();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> V value(String key) throws NoSuchElementException {
		return (V) get(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<V> values(String... propertyKeys) {
		return (Iterator<V>) Arrays.stream(propertyKeys).map(this::get).iterator();
	}
	
	@Override
    public String toString() {
    	return StringFactory.vertexString(this);
    }

}

