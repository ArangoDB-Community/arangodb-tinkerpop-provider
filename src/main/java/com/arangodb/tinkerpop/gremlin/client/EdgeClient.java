package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import org.apache.tinkerpop.gremlin.structure.Graph;

public interface EdgeClient {

    /**
     * Retrieve the 'from' vertex of an edge
     * @return
     */
    ArangoCursor<ArangoDBVertex> getEdgeFromVertex(ArangoDBEdge edge);

    /**
     * Retrieve the 'to' vertex of an edge
     * @return
     */
    ArangoCursor<ArangoDBVertex> getEdgeToVertex(ArangoDBEdge edge);

    /**
     * Remove the edge from the graph
     * @param edge                  the edge to remove
     */
    void remove(ArangoDBEdge edge);

    /**
     * Update the edge. This methods persists any new information in the DB.
     * @param arangoDBEdge
     */
    void update(ArangoDBEdge arangoDBEdge);

    /**
     * Get the graph that elements handled by this client are within.
     *
     * @return the graph of this element
     */
    Graph graph();


}

