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
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;


/**
 * The ArangoDB vertex class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBVertex extends BaseArngDocument implements Vertex, ArngElement {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);

	/** All property access is delegated to the property manager */

	protected ArangoDBPropertyManager pManager;

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
		this(null, key, null, label, null);
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
		super(id, key, rev, label);
		this.client = client;
		pManager = new ArangoDBPropertyManager(this);
	}

	// FIXME Move to interface
	public ArangoDBVertex useClient(VertexClient client) {
		return new ArangoDBVertex(_id, _key, _rev, label, client);
	}

	@Override
	public Object id() {
		try {
			return handle();
		} catch (ElementNotPairedException e) {
			throw new IllegalStateException("Id of unpaired elements can't be retrieved.", e);
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
	        	Matcher m = ArangoDBUtil.DOCUMENT_KEY.matcher((String)id);
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
		return pManager.vertexProperty(key);
	}

	@Override
	public <V> VertexProperty<V> property(
		Cardinality cardinality,
		String key,
		V value,
		Object... keyValues) {
		logger.debug("setting vertex property {} = {} ({})", key, value, keyValues);
		ElementHelper.validateProperty(key, value);
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		VertexProperty<V> p;
		if (cardinality.equals(VertexProperty.Cardinality.single)) {
			p = pManager.vertexProperty(key, value);
			addNestedProperties(p, keyValues);
			ElementHelper.attachProperties(p, keyValues);
		}
		// FIXME This assumes Cardinality is not changed from set to list (and viceversa)
		else {
			p = pManager.vertexProperty(key, value, cardinality);
			Collection<VertexProperty<V>> matches = pManager.propertiesForValue(key, value);
			if (matches.isEmpty()) {
				ElementHelper.attachProperties(p, keyValues);
			}
			else {
				for (VertexProperty<V> m : matches) {
					p = m;
					ElementHelper.attachProperties(m, keyValues);
				}
			}
		}
		client.update(this);
		return p;
	}


	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
		logger.debug("Get Properties {}", (Object[])propertyKeys);
		return pManager.vertexProperties(propertyKeys);
	}

	@Override
	public <V> Iterator<V> values(String... propertyKeys) {
		logger.debug("Get Values {}", (Object[])propertyKeys);
		return pManager.values(propertyKeys);
	}

	@Override
	public Set<String> keys() {
		return pManager.keys();
	}

	@Override
	public void removeProperty(ArangoDBElementProperty<?> property) {
		this.pManager.removeProperty(property);
	}

	@Override
	public void attachProperties(String key, Collection<ArangoDBVertexProperty> properties) {
		this.pManager.attachVertexProperties(key, properties);
	}

	/**
	 * Add the nested vertexProperties to the vertex property
	 * @param p             the VertexProperty
	 * @param keyValues     the pairs of nested primaryKey:value to add
	 */
	private void addNestedProperties(VertexProperty<?> p, Object[] keyValues) {
		for (int i = 0; i < keyValues.length; i = i + 2) {
			if (!keyValues[i].equals(T.id) && !keyValues[i].equals(T.label)) {
				p.property((String)keyValues[i], keyValues[i + 1]);
			}
		}
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

