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


public class ArangoDBVertexProperty<V> extends ArangoDBElementProperty<V> implements VertexProperty<V> {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertexProperty.class);

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBVertexProperty() {
		super();
	}


	public ArangoDBVertexProperty(String key, V value, ArangoDBBaseDocument owner) {
		super(key, value, owner, ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION);
	}

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
        ArangoDBGraph.ArangoDBIterator<ArangoDBVertex> iterator = new ArangoDBGraph.ArangoDBIterator<ArangoDBVertex>(graph, q.getCursorResult(ArangoDBVertex.class));
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
        labels.add(ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION);
        ArangoDBPropertyFilter filter = new ArangoDBPropertyFilter();
        for (String pk : propertyKeys) {
            filter.has("key", pk, ArangoDBPropertyFilter.Compare.EQUAL);
        }
        ArangoDBQuery query = graph.getClient().getDocumentNeighbors(graph, this, labels, Direction.OUT, filter);
        return new ArangoDBGraph.ArangoDBIterator<Property<U>>(graph, query.getCursorResult(ArangoDBPropertyProperty.class));
	}

}