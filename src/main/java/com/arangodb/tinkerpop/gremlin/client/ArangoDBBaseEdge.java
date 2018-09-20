//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.entity.DocumentField;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

/**
 * The ArangoDB BaseEdge provides the internal fields required for the driver to correctly 
 * serialize and deserialize edges.
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public abstract class ArangoDBBaseEdge extends ArangoDBBaseDocument {

    
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

    /**
     * Instantiates a new Arango DB base edge.
     *
     * @param from 			the from/source vertex id
     * @param to 			the to/target vertex id
     * @param graph 		the graph
     * @param collection 	the collection where the edge should be added
     */
    public ArangoDBBaseEdge(String from, String to, ArangoDBGraph graph, String collection) {
        this(from, to, null, graph, collection);
    }

    /**
     * Instantiates a new Arango DB base edge.
     *
     * @param from 			the from/source vertex id
     * @param to 			the to/target vertex id
     * @param key 			the key to assing to the edge
     * @param graph 		the graph
     * @param collection 	the collection where the edge should be added
     */
    public ArangoDBBaseEdge(String from, String to, String key, ArangoDBGraph graph, String collection) {
        super(key);
        this._from = from;
        this._to = to;
        this.graph = graph;
        this.collection = collection;
    }

    /**
     * Return the id of the from/source vertex.
     *
     * @return the id of the vertex
     */
    public String _from() {
        return _from;
    }

    /**
     * Change the from/source vertex.
     *
     * @param from 			the from/source vertex id
     */
    public void _from(String from) {
        this._from = from;
    }

    /**
     * Return the id of the to/target vertex.
     *
     * @return the id of the vertex
     */
    public String _to() {
        return _to;
    }

    /**
     * Change the to/target vertex.
     *
     * @param to 			the to/target vertex id
     */
    public void _to(String to) {
        this._to = to;
    }
}
