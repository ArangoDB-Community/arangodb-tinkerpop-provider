package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.entity.DocumentField;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ArangoDBBaseEdge extends ArangoDBBaseDocument {

    private static final Logger logger = LoggerFactory.getLogger(ArangoDBBaseEdge.class);


    /** ArangoDB internal from. */

    @DocumentField(DocumentField.Type.FROM)
    private String _from;

    /** ArangoDB internal to. */

    @DocumentField(DocumentField.Type.TO)
    private String _to;

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

    public ArangoDBBaseEdge() {
        super();
    }

    public ArangoDBBaseEdge(String _from, String _to, ArangoDBGraph graph, String collection) {
        this(_from, _to, null, graph, collection);
    }

    public ArangoDBBaseEdge(String _from, String _to, String _key, ArangoDBGraph graph, String collection) {
        super(_key);
        this._from = _from;
        this._to = _to;
        this.graph = graph;
        this.collection = collection;
    }

    public String _from() {
        return _from;
    }

    public void _from(String from) {
        this._from = from;
    }

    public String _to() {
        return _to;
    }

    public void _to(String to) {
        this._to = to;
    }
}
