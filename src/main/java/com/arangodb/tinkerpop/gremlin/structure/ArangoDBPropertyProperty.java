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
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBIterator;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

/**
 * The Class ArangoDBPropertyProperty.
 *
 * @param <U> the type of the property value
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public class ArangoDBPropertyProperty<U> extends ArangoDBElementProperty<U> {
    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

    public ArangoDBPropertyProperty() {
        super();
    }

    /**
     * Instantiates a new ArangoDB property property.
     *
     * @param name                  the name of the property
     * @param value                 the value of the property
     * @param owner                 the owner of the property
     */
    
    public ArangoDBPropertyProperty(String name, U value, ArangoDBBaseDocument owner) {
        super(name, value, owner, ArangoDBGraph.ELEMENT_PROPERTIES_COLLECTION);
    }
    
    @SuppressWarnings("rawtypes")
	@Override
    public Element element() {
        ArangoCursor<ArangoDBVertexProperty> q = graph.getClient()
        		.getDocumentNeighbors(this, Collections.emptyList(), Direction.IN, ArangoDBPropertyFilter.empty(), ArangoDBVertexProperty.class);
		ArangoDBIterator<ArangoDBVertexProperty> iterator = new ArangoDBIterator<ArangoDBVertexProperty>(graph, q);
        return iterator.hasNext() ? iterator.next() : null;
    }
    
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
    	return key().hashCode() + value().hashCode();
    }

}
