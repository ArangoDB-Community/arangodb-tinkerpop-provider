package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;

public interface ArangoGraph {

    /**
     * Insert an ArangoDBGraphVariables document for a given graph.
     * @param variables 				the variables
     * @throws ArangoDBGraphException 	If there was an error inserting the document or if the variables
     *      are already paired.
     * @throws IllegalArgumentException If graph variables already exist for the graph
     */
    ArangoDBGraphVariables insertGraphVariables(ArangoDBGraphVariables variables, ArangoDBGraph graph);

    /**
     * Get the graph variables for the given graph
     * @param graph
     * @return the variables
     */
    ArangoDBGraphVariables getGraphVariables(ArangoDBGraph graph);

    /**
     * Update the document in the graph.
     * @param variables 				the document
     *
     * @throws ArangoDBGraphException 	If there was an error updating the document
     */

    ArangoDBGraphVariables updateGraphVariables(ArangoDBGraphVariables variables, ArangoDBGraph graph);

    /**
     * Delete the graph variables for the given graph
     * @param variables            	the variables
     * @throws ArangoDBGraphException 	If there was an error deleting the document
     */

    void deleteGraphVariables(ArangoDBGraphVariables variables, ArangoDBGraph graph);
}
