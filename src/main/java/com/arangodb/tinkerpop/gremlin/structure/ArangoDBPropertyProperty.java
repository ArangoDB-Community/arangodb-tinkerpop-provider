//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.Collections;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

/**
 * The Class ArangoDBPropertyProperty.
 *
 * @param <U> the type of the property value
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */
public class ArangoDBPropertyProperty<U> extends ArangoDBElementProperty<U> {
    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

    public ArangoDBPropertyProperty() {
        super();
    }

    /**
     * Instantiates a new arango DB property property.
     *
     * @param key the key
     * @param value the value
     * @param owner the owner
     */
    
    public ArangoDBPropertyProperty(String key, U value, ArangoDBBaseDocument owner) {
        super(key, value, owner, ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION);
    }
    
    @Override
    public Element element() {
        ArangoDBQuery q = graph.getClient().getDocumentNeighbors(graph, this, Collections.emptyList(), Direction.IN, null);
        @SuppressWarnings("unchecked")
		ArangoDBIterator<ArangoDBVertexProperty<?>> iterator = new ArangoDBIterator<ArangoDBVertexProperty<?>>(graph, q.getCursorResult(ArangoDBVertexProperty.class));
        return iterator.hasNext() ? (Element) iterator.next() : null;
    }

}
