package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collections;

public class ArangoDBEdgeProperty<U> extends ArangoDBElementProperty<U> {

    @Override
    public Element element() {
        ArangoDBQuery q = graph.getClient().getDocumentNeighbors(graph, this, Collections.emptyList(), Direction.IN, null);
        ArangoDBGraph.ArangoDBIterator<ArangoDBEdge> iterator = new ArangoDBGraph.ArangoDBIterator<ArangoDBEdge>(graph, q.getCursorResult(ArangoDBEdge.class));
        return iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

    public ArangoDBEdgeProperty() {
        super();
    }

    public ArangoDBEdgeProperty(String key, U value, ArangoDBBaseDocument owner) {
        super(key, value, owner, ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION);
    }
}
