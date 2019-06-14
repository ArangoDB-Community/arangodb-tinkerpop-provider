package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.GraphCreateOptions;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;

import java.util.List;

/**
 * A client to interact with an ArangoDB graph
 */
public interface GraphClient {

    class GraphVariablesNotFoundException extends Exception {
        public GraphVariablesNotFoundException(String message) {
            super(message);
        }
    }

    /**
     * Insert an ArangoDBGraphVariables document for a given graph.
     * @param variables                the variables
     * @throws ArangoDBGraphException 	If there was an error inserting the document or if the variables
     *      are already paired.
     * @throws IllegalArgumentException If graph variables already exist for the graph
     */
    ArangoDBGraphVariables insertGraphVariables(ArangoDBGraphVariables variables);

    /**
     * Get the graph variables for the given graph
     * @return the variables
     */
    ArangoDBGraphVariables getGraphVariables() throws GraphVariablesNotFoundException;

    /**
     * Update the graph variables
     * @param variables 				the document
     *
     * @throws ArangoDBGraphException 	If there was an error updating the document
     */

    void updateGraphVariables(ArangoDBGraphVariables variables) throws GraphVariablesNotFoundException;

    /**
     * Delete the graph variables for the given graph
     * @param variables                the variables
     * @throws ArangoDBGraphException 	If there was an error deleting the document
     */

    void deleteGraphVariables(ArangoDBGraphVariables variables);

    /**
     * Pairs this graph with a database graph. If the graph does not exist, it will create one.
     * If the graph exists, it will verify that it matches the edge definitions and vertex collections.
     * @param vrtxCollections   the names of the vertex collections
     * @param edgeDefinitions       the edge definitions of the graph
     * @param options               the graph create options
     * @return  A new GraphClient that is paired to the underlying database graph
     * @throws DatabaseClient.GraphCreationException if the graph does not exist and there is an error creating it.
     */
    GraphClient pairWithDatabaseGraph(
            List<String> vrtxCollections,
            List<EdgeDefinition> edgeDefinitions,
            GraphCreateOptions options) throws DatabaseClient.GraphCreationException;

    /**
     * Return the collection name correctly prefixed according to the shouldPrefixCollectionNames flag
     * @param collectionName        the collection name to prefix
     * @return
     */

    String getPrefixedCollectioName(String collectionName);

    /**
     * Returns true if the graph has been paired with an instance in the DB.
     * @see #pairWithDatabaseGraph(List, List, GraphCreateOptions)
     * @return
     */
    boolean isPaired();
}
