//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseEdge;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph.ArangoDBIterator;
import com.arangodb.velocypack.annotations.Expose;

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
	
	/**  Map to store the element properties */
	
	@Expose(serialize = false, deserialize = false)
	protected Map<String, ArangoDBElementProperty<?>> properties = new HashMap<>(4, 0.75f);

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


	@SuppressWarnings("unchecked")
	@Override
	public <V> Property<V> property(String key, V value) {
		logger.info("property {} = {}", key, value);
		ElementHelper.validateProperty(key, value);

        ArangoDBEdgeProperty<V> p;       // = (ArangoDBElementProperty<V>) property(key);
        Optional<Property<V>> op = Optional.of(property(key));
		if (!op.isPresent()) {
            p = ArangoDBUtil.createArangoDBEdgeProperty(key, value, this);
        }
		else {
		    p = (ArangoDBEdgeProperty<V>) op.get();
			p.value(value);
		}
		return p;
	}

	@Override
	public void remove() {
		logger.info("removing {} from graph {}.", this._key(), graph.name());
		graph.getClient().deleteEdge(graph, this);
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
		return new ArangoDBIterator<Vertex>(graph, query.getCursorResult(ArangoDBVertex.class));
	}
	
	/**
	 * Removing a property while iterating will throw ConcurrentModificationException 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        List<String> labels = new ArrayList<>();
        labels.add(ArangoDBUtil.getCollectioName(graph.name(), ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION));
        ArangoDBPropertyFilter filter = new ArangoDBPropertyFilter();
        for (String pk : propertyKeys) {
            filter.has("key", pk, ArangoDBPropertyFilter.Compare.EQUAL);
        }
        ArangoDBQuery query = graph.getClient().getDocumentNeighbors(graph, this, labels, Direction.OUT, filter);
        return new ArangoDBIterator<Property<V>>(graph, query.getCursorResult(ArangoDBElementProperty.class));
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
