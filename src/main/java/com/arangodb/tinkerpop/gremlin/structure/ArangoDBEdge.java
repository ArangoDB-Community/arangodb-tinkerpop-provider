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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.entity.DocumentField;
import com.arangodb.entity.DocumentField.Type;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph.ArangoDBIterator;
import com.arangodb.velocypack.annotations.Expose;

/**
 * The ArangoDB edge class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBEdge extends AbstractArangoDBElement implements Edge {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBEdge.class);

//	public static final String FROM_KEY = "arangodb_from_key";
//	public static final String TO_KEY = "arangodb_to_key";
//	public static final String CARDINALITY = "arangodb_property_cardinality";
//	public static final String VALUE = "arangodb_property_value";
//	public static final String PROPERTIES = "arangodb_vertex_properties";

	/** ArangoDB internal from. */
	
	@DocumentField(Type.FROM)
	private String _from;

	/** ArangoDB internal to. */
	
	@DocumentField(Type.TO)
	private String _to;
	
	/** Tinkerpop ids are managed through keys, so we need to keep that information */
	
	private String from_key;

	/** Tinkerpop ids are managed through keys, so we need to keep that information */
	
	private String to_key;
	
	/**  Map to store the element properties */
	
	@Expose(serialize = false, deserialize = false)
	protected Map<String, ArangoDBElementProperty<?>> properties = new HashMap<>(4, 0.75f);
	
	
	public ArangoDBEdge() {
		super();
	}

	public ArangoDBEdge(ArangoDBGraph graph, String collection, String key, ArangoDBVertex from, ArangoDBVertex to) {
		super(graph, collection, key);
		this._from = from._id();
		this._to = to._id();
		this.from_key = from._key();
		this.to_key = to._key();
	}

	public ArangoDBEdge(ArangoDBGraph graph, String collection, ArangoDBVertex from, ArangoDBVertex to) {
		this(graph, collection, null, from, to);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> Property<V> property(String key, V value) {
		logger.info("property {} = {}", key, value);
		ElementHelper.validateProperty(key, value);
		ArangoDBElementProperty<V> p = (ArangoDBElementProperty<V>) property(key);
		if (p == null) {
			p = new ArangoDBElementProperty<V>(key, value, this);
		}
		else {
			V oldValue = p.value(value);
			if ((oldValue != null) && !oldValue.equals(value)) {
				save();
			}
		}
		return p;
	}
	
	
	@Override
	public void remove() {
		logger.info("removing {} from graph {}.", this._key(), graph.name());
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
	
	/**
	 * Removing a property while iterating will throw ConcurrentModificationException 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<Property<V>> properties(String... propertyKeys) {
		
		Set<String> allProperties = new HashSet<>(properties.keySet());
		if (propertyKeys.length > 1) {
			allProperties.retainAll(Arrays.asList(propertyKeys));
		}
		return properties.entrySet().stream()
				.filter(e -> allProperties.contains(e.getKey()))
				.map(e -> e.getValue())
				.map(p -> (Property<V>)p)
				.iterator();
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public <V> Property<V> property(String key) {
		return (Property<V>) properties.get(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> V value(String key) throws NoSuchElementException {
		return (V) properties.get(key).value();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<V> values(String... propertyKeys) {
		// FIXME Is this a filtering operation too?
		return (Iterator<V>) Arrays.stream(propertyKeys).map(this.properties::get).iterator();
	}	
	
	@Override
	public Set<String> keys() {
		logger.debug("keys");
		return Collections.unmodifiableSet(properties.keySet());
	}
   

	public void removeProperty(String key) {
		this.properties.remove(key);
		save();
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

	@Override
	public void removeProperty(ArangoDBElementProperty<?> property) {
		ArangoDBElementProperty<?> oldValue = this.properties.remove(property.key());
		if (oldValue != null) {
			save();
		}
		
	}

	@Override
	public Set<String> propertiesKeys() {
		return this.properties.keySet();
	}
	
	
	public String _from() {
		return _from;
	}

	public void _from(String from) {
		this._from = from;
	}

	public String _to() {
		return _to;
	}

	public void _to(String to) {
		this._to = to;
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
	
	
	
}
