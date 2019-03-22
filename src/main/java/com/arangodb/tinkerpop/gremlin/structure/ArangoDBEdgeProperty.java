//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.Collections;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBIterator;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

/**
 * The Class ArangoDBEdgeProperty.
 *
 * @param <V> the generic type
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public class ArangoDBEdgeProperty<V> extends ArangoDBElementProperty<V> {

    /**
     * Constructor used for Arabgo DB JavaBeans serialisation.
     */

    public ArangoDBEdgeProperty() {
        super();
    }

    /**
     * Instantiates a new Arango DB edge property.
     *
     * @param key                   the name of the property
     * @param value                 the value of the property
     * @param owner                 the owner of the property
     */
    
    public ArangoDBEdgeProperty(String key, V value, ArangoDBBaseDocument owner) {
        super(key, value, owner, ArangoDBGraph.ELEMENT_PROPERTIES_COLLECTION);
    }
    
    @Override
    public Element element() {
        ArangoCursor<ArangoDBEdge> q = graph.getClient().getDocumentNeighbors( this, Collections.emptyList(), Direction.IN, ArangoDBPropertyFilter.empty(), ArangoDBEdge.class);
		ArangoDBIterator<ArangoDBEdge> iterator = new ArangoDBIterator<ArangoDBEdge>(graph, q);
        return iterator.hasNext() ? iterator.next() : null;
    }
}
