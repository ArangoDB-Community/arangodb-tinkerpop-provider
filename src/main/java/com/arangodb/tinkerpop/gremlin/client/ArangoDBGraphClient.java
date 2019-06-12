package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;

public interface ArangoDBGraphClient {

    /**
     * Load the database
     * @return
     */
    PlainClient load();

    /**
     * Connect to a specific ArangoDB database. If the client was previously connected to a DB, all queries on the old
     * DB will be terminated and the client linked to the new DB.
     */

    ArangoDBGraphClient connectTo(String dbname, boolean createDatabase);

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

    /**
     * Get the version of the underlying database
     * @return A string with the version number
     * @throws ArangoDBGraphException
     */

    String getVersion() throws ArangoDBGraphException;

    /**
     * Indicates if the ArangoDBGraphClient is ready to be used. When using the ArangoDBGraphClient this method should
     * be invoked before calling any API to make sure that it is ready to use the underlying DB.
     *
     * @return true if the ArangoDBGraphClient is ready to be used.
     */

    boolean isReady();

    /**
     * Returns true if the specified DB that the client uses exists.
     * @see #connectTo(String, boolean)
     * @return true
     * @throws ArangoDBGraphException if the client is not connected to a DB
     *
     */
    boolean dbExists();

    /**
     * Deletes the DB from the ArangoDB server
     * @throws ArangoDBGraphException
     */
    void deleteDb() throws ArangoDBGraphException;
//
//    /**
//     * Get the graph variables associated with the graph used by this ArangoDBGraphClient. GraphVariables are stored
//     * as a single document.
//     * @return An ArangoDBGraphVariables instance that contains the graph variables.
//     */
//    ArangoDBGraphVariables getGraphVariables();
//
//    /**
//     * Insert graph variables for the graph used by this ArangoDBGraphClient.
//     * @param document
//     * @throws ArangoDBGraphException if the variables are not for the graph.
//     */
//    void insertGraphVariables(ArangoDBGraphVariables document);
//
//    /**
//     * Delete the graph variables for the graph used by this ArangoDBGraphClient
//     * @param document
//     * @throws ArangoDBGraphException if the variables are not for the graph.
//     */
//    void deleteGraphVariables(ArangoDBGraphVariables document);
//
//    /**
//     * Update the graph variables for the graph used by this ArangoDBGraphClient
//     * @param document
//     * @throws ArangoDBGraphException if the variables are not for the graph.
//     */
//    void updateGraphVariables(ArangoDBGraphVariables document);
}
