package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ArngVertexClient implements VertexClient {

    private static final Logger logger = LoggerFactory.getLogger(ArngVertexClient.class);

    private final GraphClient graphClient;
    private final ArngGraph graph;
    private final EdgeClient edgeClient;

    public ArngVertexClient(
        GraphClient graphClient,
        ArngGraph graph) {
        this.graphClient = graphClient;
        this.graph = graph;
        this.edgeClient = new ArngEdgeClient(graphClient, graph);
    }

    @Override
    public void remove(ArangoDBVertex vertex) {
        graphClient.remove(vertex);
    }

    @Override
    public void update(ArangoDBVertex vertex) {
        graphClient.update(vertex);
    }

    @Override
    public ArngGraph graph() {
        return graph;
    }

    @Override
    public ArngEdge createEdge(
        String key,
        String label,
        ArangoDBVertex from,
        ArangoDBVertex to,
        Object... keyValues) {
        return graphClient.insertEdge(key, label, from, to, edgeClient, keyValues);
    }

    @Override
    public Iterator<Edge> edges(
        ArangoDBVertex vertex,
        Direction direction,
        String[] edgeLabels) {
        logger.debug("Get Vertex's {}:{} Edges, in {}, from collections {}", vertex.id(), direction, graph.name(), edgeLabels);
        Collection<String> edgeCollections = getQueryEdgeCollections(edgeLabels);
		// If edgeLabels was not empty but all were discarded, this means that we should return an empty iterator,
        // i.e. no edges for that edgeLabels exist.
		if (edgeCollections.isEmpty()) {
			return Collections.emptyIterator();
		}
		Map<String, Object> bindVars = new HashMap<>();
		ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
		ArangoDBQueryBuilder.Direction arangoDirection = getDirection(direction);
		logger.debug("Creating query");
		try {
            queryBuilder.iterateGraph(graph.name(), "v", Optional.of("e"),
                    Optional.empty(), Optional.empty(), Optional.empty(),
                    arangoDirection, vertex.handle(), bindVars)
                    .graphOptions(Optional.of(ArangoDBQueryBuilder.UniqueVertices.NONE), Optional.empty(), true)
                    .filterSameCollections("e", edgeCollections, bindVars)
                    .ret("e");
        }
		catch (ArngDocument.ElementNotPairedException e) {
            throw new IllegalStateException("Error retrieving vertex edges", e);
        }
		String query = queryBuilder.toString();
		return new EdgeIterator(edgeClient, graphClient.executeAqlQuery(query, bindVars, null, ArangoDBEdge.class));
    }

    @Override
    public Iterator<Vertex> vertices(
            ArangoDBVertex vertex,
            Direction direction,
            String[] edgeLabels) {
        logger.debug("Get Vertex's {}:{} vertices, in {}, from collections {}", vertex.id(), direction, graph.name(), edgeLabels);
        Collection<String> edgeCollections = getQueryEdgeCollections(edgeLabels);
        // If edgeLabels was not empty but all were discarded, this means that we should return an empty iterator,
        // i.e. no edges for that edgeLabels exist.
        if (edgeCollections.isEmpty()) {
            return Collections.emptyIterator();
        }
		Map<String, Object> bindVars = new HashMap<>();
		ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
		ArangoDBQueryBuilder.Direction arangoDirection = getDirection(direction);
        // ArangoDBPropertyFilter.empty()
        try {
            queryBuilder.iterateGraph(graph.name(), "v", Optional.of("e"),
                    Optional.empty(), Optional.empty(), Optional.empty(),
                    arangoDirection, vertex.handle(), bindVars)
                    .graphOptions(Optional.of(ArangoDBQueryBuilder.UniqueVertices.GLOBAL), Optional.empty(), true)
                    .filterSameCollections("e", edgeCollections, bindVars)
                    .filterProperties(ArangoDBPropertyFilter.empty(), "v", bindVars)
                    .ret("v");
        }
        catch (ArngDocument.ElementNotPairedException e) {
            throw new IllegalStateException("Error retrieving vertex neighbours", e);
        }
		String query = queryBuilder.toString();
        return new VertexIterator(this, graphClient.executeAqlQuery(query, bindVars, null, ArangoDBVertex.class));
    }

    /**
     * Query will raise an exception if the edge_collection name is not in the graph, so we need to filter out
     * edgeLabels not in the graph.
     *
     * @param edgeLabels
     * @return
     */
    private Collection<String> getQueryEdgeCollections(String... edgeLabels) {
        Collection<String> vertexCollections;
        if (edgeLabels.length == 0) {
            vertexCollections = graph.edgeCollections();
        }
        else {
            vertexCollections = Arrays.stream(edgeLabels)
                    .filter(el -> graph.edgeCollections().contains(el))
                    .map(graph::getDBCollectionName)
                    .collect(Collectors.toList());

        }
        return vertexCollections;
    }

    private ArangoDBQueryBuilder.Direction getDirection(Direction direction) {
		ArangoDBQueryBuilder.Direction arangoDirection;
		switch (direction) {
			case IN:
				arangoDirection = ArangoDBQueryBuilder.Direction.IN;
				break;
			case OUT:
				arangoDirection = ArangoDBQueryBuilder.Direction.OUT;
				break;
			case BOTH:
			default:
				arangoDirection = ArangoDBQueryBuilder.Direction.ALL;
				break;
		}
		return arangoDirection;
	}
}
