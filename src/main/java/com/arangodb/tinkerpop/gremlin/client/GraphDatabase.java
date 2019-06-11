package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDatabase;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

public interface GraphDatabase {

    /**
     * Load the database
     * @return
     */
    ArangoGraphDatabase load();

    /**
     * Connect to a specific ArangoDB database
     */
    GraphDatabase connectTo(String dbname, boolean createDatabase);

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

    /**
     * Indicates if the GraphDatabase is ready to be used.
     *
     * @return true if the GraphDatabase is ready to be used.
     */
    boolean isReady();

    boolean dbExists();

    void deleteDb() throws ArangoDBGraphException;
}
