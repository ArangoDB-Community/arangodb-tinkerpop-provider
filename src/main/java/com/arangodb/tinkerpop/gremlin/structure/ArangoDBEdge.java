//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.arangodb.tinkerpop.gremlin.client.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;


/**
 * The ArangoDB Edge class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBEdge extends ArangoDBBaseEdge implements Edge {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBEdge.class);

    /**
     * Constructor used for ArabgoDB JavaBeans de-/serialisation.
     */

	public ArangoDBEdge() { super(); }

    /**
     * Create a new ArangoDBEdge that connects the given vertices. The edge name can be provided.
     * @param key           		the edge name
	 * @param label            		the edge label
	 * @param from         		 	the source vertex
	 * @param to            		the target vertex
	 * @param graph         		the graph in which the edge is created
	 */

	public ArangoDBEdge(
		String key,
		String label,
		ArangoDBVertex from,
		ArangoDBVertex to,
		ArangoDBGraph graph) {
		super(key, label, from._id(), to._id(), graph);
	}

    /**
     * Create a new ArangoDBEdge that connects the given vertices.
     *
     * @param graph         		the graph in which the edge is created
     * @param label    				the label into with the edge is created
     * @param from          		the source vertex
     * @param to            		the target vertex
     */

	public ArangoDBEdge(
	    ArangoDBGraph graph,
        String label,
        ArangoDBVertex from,
        ArangoDBVertex to) {
		this(null, label, from, to, graph);
	}

    @Override
    public Object id() {
        return _id();
    }

    @Override
    public String label() {
        return label;
    }


	@Override
	public <V> Property<V> property(
		String key,
		V value) {
		logger.info("set property {} = {}", key, value);
		ElementHelper.validateProperty(key, value);
		Property<V> p = property(key);
		if (!p.isPresent()) {
            p = ArangoDBUtil.createArangoDBEdgeProperty(key, value, this);
        }
		else {
			((ArangoDBEdgeProperty<V>) p).value(value);
		}
		return p;
	}

	@Override
	public void remove() {
		logger.info("removing {} from graph {}.", this._key(), graph.name());
		if (paired) {
			try {
				graph.getClient().deleteEdge(this);
			} catch (ArangoDBGraphException ex) {

			}
		}
	}

	@Override
	public Iterator<Vertex> vertices(Direction direction) {
		boolean from = true;
		boolean to = true;
		switch(direction) {
		case BOTH:
			break;
		case IN:
			from = false;
			break;
		case OUT:
			to = false;
			break;
		}
		String edgeCollection = isPaired() ? label() : graph.getPrefixedCollectioName(label());
		return new ArangoDBIterator<>(graph, graph.getClient().getEdgeVertices(_id(), edgeCollection, from, to));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        List<String> labels = new ArrayList<>();
		labels.add(graph.getPrefixedCollectioName(ArangoDBGraph.ELEMENT_PROPERTIES_EDGE_COLLECTION));
        ArangoDBPropertyFilter filter = new ArangoDBPropertyFilter();
        for (String pk : propertyKeys) {
            filter.has("name", pk, ArangoDBPropertyFilter.Compare.EQUAL);
        }
        ArangoCursor<?> documentNeighbors = graph.getClient().getElementProperties(this, labels, filter, ArangoDBEdgeProperty.class);
		return new ArangoDBPropertyIterator<>(graph, (ArangoCursor<ArangoDBEdgeProperty<V>>) documentNeighbors);
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
