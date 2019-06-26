//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.client.VertexClient;
import com.arangodb.tinkerpop.gremlin.structure.properties.ArngVertexProperties;
import com.arangodb.tinkerpop.gremlin.structure.properties.VertexProperties;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * The ArangoDB vertex class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBVertex extends BaseArngDocument implements ArngVertex {

	public static class CantAddValueToSinglePropertyException extends Exception {

		public CantAddValueToSinglePropertyException(String message) {
			super(message);
		}
	}

	public static class CantRemoveValueFromSinglePropertyException extends Exception {

		public CantRemoveValueFromSinglePropertyException(String message) {
			super(message);
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);

	// TODO Move to ONE place
	private static final Pattern DOCUMENT_KEY = Pattern.compile("^[A-Za-z0-9_:\\.@()\\+,=;\\$!\\*'%-]*");

	/** All property access is delegated to the property manager */

	protected VertexProperties properties;

	private final VertexClient client;

	/**
	 * Instantiates a new arango DB vertex.
	 *
	 * @param key					the vertex's key
	 * @param label 				the vertex's label
	 */
	public ArangoDBVertex(
		String key,
		String label) {
		this(null, key, null, label, null, new ArngVertexProperties());
	}

	/**
	 * Instantiates a new arango DB vertex.
	 *
	 * @param id					the edge handle
	 * @param key					the edge primary key
	 * @param rev					the edge revision
	 * @param label					the edge label
	 * @param client				the client
	 */
	public ArangoDBVertex(
			String id,
			String key,
			String rev,
			String label,
			VertexClient client) {
		this(id, key, rev, label, client,new ArngVertexProperties());
	}

	/**
	 * Instantiates a new arango DB vertex.
	 *
	 * @param id					the edge handle
	 * @param key					the edge primary key
	 * @param rev					the edge revision
	 * @param label					the edge label
	 * @param client				the client
	 */
	public ArangoDBVertex(
		String id,
		String key,
		String rev,
		String label,
		VertexClient client,
		VertexProperties properties) {
		super(id, key, rev, label);
		this.client = client;
		this.properties = properties;
	}

	// FIXME Move to interface
	public ArangoDBVertex useClient(VertexClient client) {
		return new ArangoDBVertex(_id, _key, _rev, label, client, properties);
	}

	@Override
	public Object id() {
		try {
			return handle();
		} catch (ElementNotPairedException e) {
			return primaryKey();
		}
	}

	@Override
	public Graph graph() {
		return client.graph();
	}

	@Override
	public void remove() {
		logger.info("removing {} from graph.", this.id());
		if (paired) {
			if (client == null) {
				throw new UnsupportedOperationException("This operation can only be called if the vertex is using a client.");
			}
			try {
				client.remove(this);
			} catch (ArangoDBGraphException e) {
				throw new IllegalStateException("Unable to remove edge", e);
			}
		}
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
		if (client == null) {
			throw new UnsupportedOperationException("This operation can only be called if the vertex is using a client.");
		}
		logger.info("addEdge in label {} to vertex {}", label, inVertex == null ? "?" :inVertex.id());
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		ElementHelper.validateLabel(label);
		if (!client.graph().hasEdgeCollection(label)) {
			throw new IllegalArgumentException(String.format("Edge label (%s) not in graph (%s) edge collections.", label, client.graph().name()));
		}
		if (inVertex == null) {
			Graph.Exceptions.argumentCanNotBeNull("vertex");
		}
		String key = null;
		if (ElementHelper.getIdValue(keyValues).isPresent()) {
        	Object id = ElementHelper.getIdValue(keyValues).get();
        	if (graph().features().edge().willAllowId(id)) {
	        	Matcher m = DOCUMENT_KEY.matcher((String)id);
        		if (!m.matches()) {
        			throw new ArangoDBGraphException(String.format("Given id (%s) has unsupported characters.", id));
            	}
        		key = id.toString();
        	}
        	else {
        		throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        	}
        }
		return client.createEdge(key, label, this, (ArangoDBVertex) inVertex, keyValues);
	}

	@Override
	public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
		if (client == null) {
			throw new UnsupportedOperationException("This operation can only be called if the vertex is using a client.");
		}
		return client.edges(this, direction, edgeLabels);
	}

	@Override
	public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
		if (client == null) {
			throw new UnsupportedOperationException("This operation can only be called if the vertex is using a client.");
		}
		return client.vertices(this, direction, edgeLabels);
	}


	@Override
	public <V> VertexProperty<V> property(final String key) {
		return properties.property(key);
	}

	@Override
	public <V> VertexProperty<V> property(final String key, final V value) {
		return properties.property(this, key, value);
	}

	@Override
	public <V> VertexProperty<V> property(
		Cardinality cardinality,
		String key,
		V value,
		Object... keyValues) {
		logger.debug("setting vertex property {} = {} ({})", key, value, keyValues);
		VertexProperty<V> result = properties.property(this, cardinality, key, value, keyValues);
		update();
		return result;
	}


	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
		logger.debug("Get Properties {}", (Object[])propertyKeys);
		return properties.properties(propertyKeys);
	}

	@Override
	public <V> Iterator<V> values(String... propertyKeys) {
		logger.debug("Get Values {}", (Object[])propertyKeys);
		return properties.values(propertyKeys);
	}

	@Override
	public Set<String> keys() {
		return properties.keys();
	}

	@Override
	public void removeProperty(Property<?> property) {
		properties.removeProperty((VertexProperty<?>) property);
	}

	@Override
	public void update() {
		client.update(this);
	}

	@Override
	public String toString() {
		return StringFactory.vertexString(this);
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

