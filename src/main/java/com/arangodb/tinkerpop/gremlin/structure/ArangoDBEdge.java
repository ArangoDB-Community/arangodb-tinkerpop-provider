//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.DocumentField;
import com.arangodb.tinkerpop.gremlin.cache.VertexLoader;
import com.arangodb.tinkerpop.gremlin.client.*;
import com.arangodb.velocypack.annotations.Expose;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.tinkerpop.gremlin.structure.*;
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

public class ArangoDBEdge extends BaseArngDocument implements ArngEdge {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBEdge.class);

	private class EdgeVertexLoader extends CacheLoader<String, ArangoDBVertex> {

		private final EdgeClient client;
		private final ArangoDBEdge edge;

		public EdgeVertexLoader(
				ArangoDBEdge edge,
				EdgeClient client) {
			this.client = client;
			this.edge = edge;
		}

		@Override
		public ArangoDBVertex load(String key) throws Exception {
			ArangoCursor<ArangoDBVertex> cursor = null;
			if (key.equals("from")) {
				cursor = client.getEdgeFromVertex(edge);
			}
			else if (key.equals("to")) {
				cursor = client.getEdgeToVertex(edge);
			}
			if (cursor == null) {
				throw new ArangoDBException("Requested unkown edge vertex");
			}
			if (!cursor.hasNext()) {
				throw new ArangoDBException("The requested edge vertex was not found");
			}
			return cursor.next();
		}
	}

	/** All property access is delegated to the property manager */

	private final ArangoDBPropertyManager pManager;

	private final LoadingCache<String, ArangoDBVertex> vertices;

	private final EdgeClient client;

	/**
	 * Create a new ArangoDBEdge that connects the given vertices.
	 * @param label    				the edge label
	 * @param from          		the source vertex
	 * @param to            		the target vertex
	 */

	public ArangoDBEdge(
		String key,
		String label,
		ArangoDBVertex from,
		ArangoDBVertex to) {
		this(null, key, null, label, from, to, null);
	}

	/**
	 *
	 * @param id					the edge handle
	 * @param key					the edge primary key
	 * @param rev					the edge revision
	 * @param label					the edge label
	 * @param from					the source vertex
	 * @param to					the target vertex
	 */

	public ArangoDBEdge(
		String id,
		String key,
		String rev,
		String label,
		ArangoDBVertex from,
		ArangoDBVertex to) {
		this(id, key, rev, label, from, to, null);
	}

	/**
	 *
	 * @param id					the edge handle
	 * @param key					the edge primary key
	 * @param rev					the edge revision
	 * @param label					the edge label
	 * @param from					the source vertex
	 * @param to					the target vertex
	 */
	public ArangoDBEdge(
		String id,
		String key,
		String rev,
		String label,
		ArangoDBVertex from,
		ArangoDBVertex to,
		EdgeClient client) {
		super(id, key, rev, label);
		this.client = client;
		vertices = CacheBuilder.newBuilder()
				.expireAfterAccess(10, TimeUnit.SECONDS)
				.build(new EdgeVertexLoader(this, client));
		vertices.put("from", from);
		vertices.put("to", to);
		pManager = new ArangoDBPropertyManager(this);
	}

	// FIXME Move to interface
	public ArangoDBEdge useClient(EdgeClient client) {
		try {
			return new ArangoDBEdge(_id, _key, _rev, label, vertices.get("from"), vertices.get("to"), client);
		} catch (ExecutionException e) {
			throw new IllegalStateException("Error assigning client to edge", e);
		}
	}

	@Override
	public String from() {
		try {
			return vertices.get("from").handle();
		} catch (ExecutionException | ElementNotPairedException e) {
			throw new IllegalStateException("Error retrieving edge 'from' vertex id.", e);
		}
	}

	@Override
	public String to() {
		try {
			return vertices.get("to").handle();
		} catch (ExecutionException | ElementNotPairedException e) {
			throw new IllegalStateException("Error retrieving edge 'to' vertex id.", e);
		}
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
			try {
				client.remove(this);
			} catch (ArangoDBGraphException e) {
				throw new IllegalStateException("Unable to remove edge", e);
			}
		}
	}

	public Vertex outVertex() {
		try {
			return vertices.get("from");
		} catch (ExecutionException e) {
			throw new IllegalStateException("Error retrieving edge vertex.", e);
		}
	}

	public Vertex inVertex() {
		try {
			return vertices.get("to");
		} catch (ExecutionException e) {
			throw new IllegalStateException("Error retrieving edge vertiex.", e);
		}
	}

	@Override
	public Iterator<Vertex> vertices(Direction direction) {
		Collection<Vertex> result = new ArrayList<>();
		try {
			switch (direction) {
				case BOTH:
					result.add(vertices.get("from"));
					result.add(vertices.get("to"));
					break;
				case IN:
					result.add(vertices.get("to"));
					break;
				case OUT:
					result.add(vertices.get("from"));
					break;
			}
		}
		catch (ExecutionException e) {
			throw new IllegalStateException("Error retrieving edge vertices.", e);
		}
		return result.iterator();
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
		Property<V> result = pManager.property(key, value);
		client.update(this);
		return result;
	}


	@Override
	public Set<String> keys() {
		return pManager.keys();
	}

	/**
	 * This method is intended for rapid deserialization
	 * @return
	 */

	public <V> void attachProperties(Iterator<Property<V>>  properties) {
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
