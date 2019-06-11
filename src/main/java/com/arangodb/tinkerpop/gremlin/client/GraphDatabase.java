package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDatabase;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

public interface GraphDatabase {

    /**
     * Setup the database to work with the specified graph.
     */
    void setup();

    /**
     * Shutdown the database.
     */

    void shutdown();

    /**
     * Remove the graph and all its elements from the database
     * @param graph
     * @throws ArangoDBGraphException
     */

    void clear(ArangoDBGraph graph) throws ArangoDBGraphException;

    String getVersion() throws ArangoDBGraphException;

}
