package com.arangodb.tinkerpop.gremlin.client;


import com.arangodb.ArangoCursor;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.GraphCreateOptions;

import java.util.List;
import java.util.Map;

/**
 * A client to interact with an ArangoDatabase
 */
public interface DatabaseClient extends AutoCloseable {

    class GraphCreationException extends Exception {
        public GraphCreationException(String message) {
            super(message);
        }
    }

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
     * @param bindVars              a map of primaryKey:baseValue for bind variables
     * @param aqlQueryOptions       AQL query options
     * @param type                  The type of the elements in the result
     * @param <T>                   The type of the elements in the result
     * @return
     * @throws ArangoDBGraphException
     */
    <T> ArangoCursor<T> executeAqlQuery(String query, Map<String, Object> bindVars, AqlQueryOptions aqlQueryOptions,
            Class<T> type) throws ArangoDBGraphException;


    /**
     * Get the graph for the given name
     * @return
     */

    ArangoGraph graph(String name);

    /**
     * Craete a new graph in the database with the given name, edge definitions and options.
     *
     * @param graphName             the graph name
     * @param edgeDefinitions       the edge definitions
     * @param options               the graph options
     * @return
     */
    ArangoGraph createGraph(
            String graphName,
            List<EdgeDefinition> edgeDefinitions,
            GraphCreateOptions options) throws GraphCreationException;

//
//    /**
//     * Connect to a specific ArangoDB database. If the client was previously connected to a DB, all queries on the old
//     * DB will be terminated and the client linked to the new DB.
//     */
//
//    DatabaseClient connectTo(String dbname, boolean createDatabase);
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
//     * Indicates if the DatabaseClient is ready to be used. When using the DatabaseClient this method should
//     * be invoked before calling any API to make sure that it is ready to use the underlying DB.
//     *
//     * @return true if the DatabaseClient is ready to be used.
//     */
//
//    boolean isReady();


//
//    /**
//     * Get the graph variables associated with the graph used by this DatabaseClient. GraphVariables are stored
//     * as a single document.
//     * @return An ArangoDBGraphVariables instance that contains the graph variables.
//     */
//    ArangoDBGraphVariables getGraphVariables();
//
//    /**
//     * Insert graph variables for the graph used by this DatabaseClient.
//     * @param document
//     * @throws ArangoDBGraphException if the variables are not for the graph.
//     */
//    void insertGraphVariables(ArangoDBGraphVariables document);
//
//    /**
//     * Delete the graph variables for the graph used by this DatabaseClient
//     * @param document
//     * @throws ArangoDBGraphException if the variables are not for the graph.
//     */
//    void deleteGraphVariables(ArangoDBGraphVariables document);
//
//    /**
//     * Update the graph variables for the graph used by this DatabaseClient
//     * @param document
//     * @throws ArangoDBGraphException if the variables are not for the graph.
//     */
//    void updateGraphVariables(ArangoDBGraphVariables document);
}
