package com.arangodb.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.Edge;

public interface ArngEdge extends ArngElement, Edge {

    /**
     * Return the id of the from/source vertex.
     *
     * @return the id of the vertex
     */

    String from();

    /**
     * Return the id of the to/target vertex.
     *
     * @return the id of the vertex
     */

    String to();

}
