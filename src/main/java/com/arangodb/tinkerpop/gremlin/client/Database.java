package com.arangodb.tinkerpop.gremlin.client;


import com.arangodb.ArangoCursor;
import com.arangodb.model.AqlQueryOptions;

import java.util.Map;

public interface Database extends AutoCloseable {

    /**
     * Get the version of the database
     * @return A string with the version number
     * @throws ArangoDBGraphException
     */

    String getVersion() throws ArangoDBGraphException;

    /**
     * Returns true if the database exists
     * @return true if the database exists
     * @throws ArangoDBGraphException if the client is not connected to a DB
     *
     */
    boolean exists();

    /**
     * Deletes the DB. This deletes the DB from the server
     * @throws ArangoDBGraphException
     */
    void delete() throws ArangoDBGraphException;

    /**
     * Execute the AQL query against the database
     * @param query                 the AQL query
     * @param bindVars              a map of key:value for bind variables
     * @param aqlQueryOptions       AQL query options
     * @param type                  The type of the elements in the result
     * @param <T>                   The type of the elements in the result
     * @return
     * @throws ArangoDBGraphException
     */
    <T> ArangoCursor<T> executeAqlQuery(String query, Map<String, Object> bindVars, AqlQueryOptions aqlQueryOptions,
            Class<T> type) throws ArangoDBGraphException;

//
//    /**
//     * Connect to a specific ArangoDB database. If the client was previously connected to a DB, all queries on the old
//     * DB will be terminated and the client linked to the new DB.
//     */
//
//    Database connectTo(String dbname, boolean createDatabase);
//
//
//
//    /**
//     * Remove the graph and all its elements from the database
//     * @param graph
//     * @throws ArangoDBGraphException
//     */
//
//    void clear(ArangoDBGraph graph) throws ArangoDBGraphException;
//
//
//
//    /**
//     * Indicates if the Database is ready to be used. When using the Database this method should
//     * be invoked before calling any API to make sure that it is ready to use the underlying DB.
//     *
//     * @return true if the Database is ready to be used.
//     */
//
//    boolean isReady();


//
//    /**
//     * Get the graph variables associated with the graph used by this Database. GraphVariables are stored
//     * as a single document.
//     * @return An ArangoDBGraphVariables instance that contains the graph variables.
//     */
//    ArangoDBGraphVariables getGraphVariables();
//
//    /**
//     * Insert graph variables for the graph used by this Database.
//     * @param document
//     * @throws ArangoDBGraphException if the variables are not for the graph.
//     */
//    void insertGraphVariables(ArangoDBGraphVariables document);
//
//    /**
//     * Delete the graph variables for the graph used by this Database
//     * @param document
//     * @throws ArangoDBGraphException if the variables are not for the graph.
//     */
//    void deleteGraphVariables(ArangoDBGraphVariables document);
//
//    /**
//     * Update the graph variables for the graph used by this Database
//     * @param document
//     * @throws ArangoDBGraphException if the variables are not for the graph.
//     */
//    void updateGraphVariables(ArangoDBGraphVariables document);
}
