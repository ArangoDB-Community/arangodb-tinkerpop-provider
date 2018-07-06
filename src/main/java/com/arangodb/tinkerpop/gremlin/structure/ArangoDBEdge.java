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
	public static final String CARDINALITY = "arangodb_property_cardinality";
	public static final String VALUE = "arangodb_property_value";
	public static final String PROPERTIES = "arangodb_vertex_properties";

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
        return new ArangoDBElementProperty<T>(next, hashCode, convertKey(key), value, this);
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
		return (ArangoDBElementProperty<PV>) getEntry(key);
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
	public void save() {
		if (paired) {
			try {
				graph.getClient().updateEdge(graph, this);
			} catch (ArangoDBGraphException e) {
				logger.error("Unable to update vertex in DB", e);
				throw new IllegalStateException("Unable to update vertex in DB", e);
			}
		}
		
	}

	@Override
    public String toString() {
    	return StringFactory.edgeString(this);
    }
	
	
	
}
