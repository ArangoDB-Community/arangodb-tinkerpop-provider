package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Collections;
import java.util.Iterator;

public class ArangoDBPropertyProperty<U> extends ArangoDBElementProperty<U> {

    @Override
    public Element element() {
        ArangoDBQuery q = graph.getClient().getDocumentNeighbors(graph, this, Collections.emptyList(), Direction.IN, null);
        ArangoDBGraph.ArangoDBIterator<ArangoDBVertexProperty> iterator = new ArangoDBGraph.ArangoDBIterator<ArangoDBVertexProperty>(graph, q.getCursorResult(ArangoDBVertexProperty.class));
        return iterator.hasNext() ? (Element) iterator.next() : null;
    }

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

    public ArangoDBPropertyProperty() {
        super();
    }

    public ArangoDBPropertyProperty(String key, U value, ArangoDBBaseDocument owner) {
        super(key, value, owner, ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION);
    }
}
