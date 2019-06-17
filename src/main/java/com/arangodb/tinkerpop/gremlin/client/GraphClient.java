package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.GraphCreateOptions;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;

import java.util.List;

/**
 * A client to interact with an ArangoDB graph
 */
public interface GraphClient extends AutoCloseable {

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


    boolean insertVertex(ArangoDBVertex vertex);
}
