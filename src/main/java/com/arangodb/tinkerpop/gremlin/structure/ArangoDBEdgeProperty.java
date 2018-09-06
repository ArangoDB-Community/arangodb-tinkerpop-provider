//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collections;

/**
 * The Class ArangoDBEdgeProperty.
 *
 * @param <U> the generic type
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */
public class ArangoDBEdgeProperty<U> extends ArangoDBElementProperty<U> {

    /**
     * Constructor used for Arabgo DB JavaBeans serialisation.
     */

    public ArangoDBEdgeProperty() {
        super();
    }

    /**
     * Instantiates a new Arango DB edge property.
     *
     * @param key the key
     * @param value the value
     * @param owner the owner
     */
    
    public ArangoDBEdgeProperty(String key, U value, ArangoDBBaseDocument owner) {
        super(key, value, owner, ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION);
    }
    
    @Override
    public Element element() {
        ArangoDBQuery q = graph.getClient().getDocumentNeighbors(graph, this, Collections.emptyList(), Direction.IN, null);
        @SuppressWarnings("unchecked")
		ArangoDBIterator<ArangoDBEdge> iterator = 
        		new ArangoDBIterator<ArangoDBEdge>(graph, q.getCursorResult(ArangoDBEdge.class));
        return iterator.hasNext() ? iterator.next() : null;
    }
}
