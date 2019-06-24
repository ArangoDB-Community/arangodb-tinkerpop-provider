package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoCursor;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;

import java.util.Map;

/**
 * A client to interact with an ArangoDB graph
 */
public interface GraphClient extends AutoCloseable {

//    /**
//     * Pairs this graph with a database graph. If the graph does not exist, it will create one.
//     * If the graph exists, it will verify that it matches the edge definitions and vertex collections.
//     * @param vrtxCollections   the names of the vertex collections
//     * @param edgeDefinitions       the edge definitions of the graph
//     * @param options               the graph create options
//     * @return  A new GraphClient that is paired to the underlying database graph
//     * @throws DatabaseClient.GraphCreationException if the graph does not exist and there is an error creating it.
//     */
//    GraphClient pairWithDatabaseGraph(
//            List<String> vrtxCollections,
//            List<EdgeDefinition> edgeDefinitions,
//            GraphCreateOptions options) throws DatabaseClient.GraphCreationException;

//    /**
//     * Returns true if the graph has been paired with an instance in the DB.
//     * @see #pairWithDatabaseGraph(List, List, GraphCreateOptions)
//     * @return
//     */
//    boolean isPaired();

    /**
     * Execute the AQL query against the database
     * @param query                 the AQL query
     * @param bindVars              a map of primaryKey:value for bind variables
     * @param aqlQueryOptions       AQL query options
     * @param type                  The type of the elements in the result
     * @param <T>                   The type of the elements in the result
     * @return
     * @throws ArangoDBGraphException
     */
    <T> ArangoCursor<T> executeAqlQuery(String query, Map<String, Object> bindVars, AqlQueryOptions aqlQueryOptions,
            Class<T> type) throws ArangoDBGraphException;

    /**
     * Insert a new vertex in the graph.
     * @param key                   the vertex's primary key (can be null)
     * @param label                 the vertex's label
     * @param keyValues             the key:value edge property pairs
     * @return  a new instance of the vertex that uses the provided client.
     */
    ArangoDBVertex insertVertex(String key, String label, Object... keyValues);

    /**
     * Remove a vertex from the graph
     * @param vertex                the vertex to remove
     */
    void remove(ArangoDBVertex vertex);

    /**
     * Update a vertex in the graph
     * @param vertex                the vertex to update
     */
    void update(ArangoDBVertex vertex);

    /**
     * Insert an edge into the graph.
     * @param key                   the edge's primary key (can be null)
     * @param label                 the edge's label (it should be properly prefixed)
     * @param from                  the source vertex of the edge
     * @param to                    the target vertex of the edge
     * @param edgeClient            the edge clien to assing to the edge
     * @param keyValues             the key:value edge property pairs
     * @return
     */
    ArangoDBEdge insertEdge(String key, String label,
        ArangoDBVertex from, ArangoDBVertex to,
        EdgeClient edgeClient, Object... keyValues);

    /**
     * Remove the edge from the graph
     * @param edge                  the edge to remove
     */
    void remove(ArangoDBEdge edge);

    /**
     * Update the edge in the grpah
     * @param edge                  the edge to update
     */
    void update(ArangoDBEdge edge);


}
