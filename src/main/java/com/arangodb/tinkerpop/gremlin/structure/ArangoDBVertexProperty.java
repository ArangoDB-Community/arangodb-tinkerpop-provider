//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Class ArangoDBVertexProperty.
 *
 * @param <V> the type of the property value
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBVertexProperty<V> extends ArangoDBElementProperty<V> implements VertexProperty<V> {

	/** The Logger. */
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertexProperty.class);

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBVertexProperty() {
		super();
	}

	/**
	 * Instantiates a new arango DB vertex property.
	 *
	 * @param key the key
	 * @param value the value
	 * @param owner the owner
	 */
	
	public ArangoDBVertexProperty(String key, V value, ArangoDBBaseDocument owner) {
		super(key, value, owner, ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION);
	}

    /**
     * Instantiates a new Arango DB vertex property.
     *
     * @param id the id
     * @param key the key
     * @param value the value
     * @param owner the owner
     */
	
    public ArangoDBVertexProperty(String id, String key, V value, ArangoDBBaseDocument owner) {
        super(id, key, value, owner, ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION);
    }

	@Override
    public String toString() {
    	return StringFactory.propertyString(this);
    }

	@Override
	public Object id() {
		return _key;
	}
	
	@Override
    public String label() {
        return key;
    }

    @Override
    public Vertex element() {
        ArangoDBQuery q = graph.getClient().getDocumentNeighbors(graph, this, Collections.emptyList(), Direction.IN, null);
        ArangoDBIterator<ArangoDBVertex> iterator = new ArangoDBIterator<ArangoDBVertex>(graph, q.getCursorResult(ArangoDBVertex.class));
        return iterator.hasNext() ? iterator.next() : null;
    }

	@Override
	public <U> Property<U> property(String key, U value) {
		logger.info("property {} = {}", key, value);
		ElementHelper.validateProperty(key, value);
        Property<U> p = property(key);
        if (!p.isPresent()) {
            p = ArangoDBUtil.createArangoDBPropertyProperty(key, value, this);
        } else {
            ((ArangoDBElementProperty<U>) p).value(value);
        }
        return p;
	}


    @SuppressWarnings("unchecked")
	@Override
	public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        List<String> labels = new ArrayList<>();
        labels.add(ArangoDBUtil.ELEMENT_PROPERTIES_EDGE);
        ArangoDBPropertyFilter filter = new ArangoDBPropertyFilter();
        for (String pk : propertyKeys) {
            filter.has("key", pk, ArangoDBPropertyFilter.Compare.EQUAL);
        }
        ArangoDBQuery query = graph.getClient().getDocumentNeighbors(graph, this, labels, Direction.OUT, filter);
        return new ArangoDBIterator<Property<U>>(graph, query.getCursorResult(ArangoDBPropertyProperty.class));
	}

}