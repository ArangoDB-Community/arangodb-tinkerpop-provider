package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.ArngEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArngGraph;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public interface VertexClient {


    /**
     * Remove the vertex from the graph
     * @param vertex                the vertex to remove
     */
    void remove(ArangoDBVertex vertex);

    /**
     * Update the vertex. This methods persists any new information in the DB.
     * @param vertex                the verex to update
     */
    void update(ArangoDBVertex vertex);

    /**
     * Get the graph that elements handled by this client are within.
     *
     * @return the graph of this element
     */
    ArngGraph graph();

    ArngEdge createEdge(String key, String label,
            ArangoDBVertex from, ArangoDBVertex to, Object... keyValues);

    /**
     * Retrieve the vertex edges for a given direction. If not empty, the edgeLabels provides a filter for the
     * dsired edge labels.
     *
     * @param direction             the direction
     * @param edgeLabels            the
     * @return
     */
    Iterator<Edge> edges(
            ArangoDBVertex vertex,
            Direction direction,
            String[] edgeLabels);

    Iterator<Vertex> vertices(
            ArangoDBVertex arangoDBVertex,
            Direction direction,
            String[] edgeLabels);
}
