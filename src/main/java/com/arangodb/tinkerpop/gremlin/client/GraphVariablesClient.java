package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;

public interface GraphVariablesClient {

    /** The default collection where graph variables are stored */

    String GRAPH_VARIABLES_COLLECTION = "TINKERPOP-GRAPH-VARIABLES";

    class GraphVariablesNotFoundException extends Exception {
        public GraphVariablesNotFoundException(String message) {
            super(message);
        }
    }

    /**
     * The graph that the variables belong to
     * @return  the name of the graph
     */
    String graphName();

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
}
