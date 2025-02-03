//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.serde.InternalFrom;
import com.arangodb.serde.InternalTo;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

/**
 * The ArangoDB BaseEdge provides the internal fields required for the driver to correctly 
 * serialize and deserialize edges.
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public abstract class ArangoDBBaseEdge extends ArangoDBBaseDocument {

    
    /** ArangoDB internal from. */

    @InternalFrom
    private String _from;

    /** ArangoDB internal to. */

    @InternalTo
    private String _to;

    /**
     * Constructor used for ArabgoDB JavaBeans de-/serialisation.
     */

    public ArangoDBBaseEdge() {
        super();
    }

    /**
     * Instantiates a new Arango DB base edge.
     *
     * @param label                 the edge label
     * @param from_id               the from/source vertex id
     * @param to_id                 the to/target vertex id
     * @param graph                 the graph
     */
    public ArangoDBBaseEdge(String label, String from_id, String to_id, ArangoDBGraph graph) {
        this(null, label, from_id, to_id, graph);
    }

    /**
     * Instantiates a new Arango DB base edge with a predefined name.
     *
     * @param key                   the name to assign to the edge
     * @param label                 the edge label
     * @param from_id               the from/source vertex id
     * @param to_id                 the to/target vertex id
     * @param graph                 the graph
     */
    public ArangoDBBaseEdge(String key, String label, String from_id, String to_id, ArangoDBGraph graph) {
        super(key, label, graph);
        this._from = from_id;
        this._to = to_id;
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
     * @param from 			        the from/source vertex id
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
     * @param to 			        the to/target vertex id
     */
    public void _to(String to) {
        this._to = to;
    }
}
