package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArngEdgeClient implements EdgeClient {

    private static final Logger logger = LoggerFactory.getLogger(ArngEdgeClient.class);

    private final GraphClient graphClient;
    private final ArngGraph graph;

    /**
     * Create a new EdgeClient for an ArangoGraph
     * @param graphClient
     * @param graph
     */
    public ArngEdgeClient(GraphClient graphClient, ArngGraph graph) {
        this.graphClient = graphClient;
        this.graph = graph;
    }

    @Override
    public ArangoCursor<ArangoDBVertex> getEdgeFromVertex(ArangoDBEdge edge) {
        logger.debug("Get edge {} 'from' vertex", edge.id());
		return getEdgeVertex(edge,"Document(e._from)");
    }

    @Override
    public ArangoCursor<ArangoDBVertex> getEdgeToVertex(ArangoDBEdge edge) {
        logger.debug("Get edge {} 'to' vertex", edge.id());
        return getEdgeVertex(edge,"Document(e._to)");
    }

    @Override
    public void remove(ArangoDBEdge edge) {
        graphClient.remove(edge);
    }

    @Override
    public void update(ArangoDBEdge edge) {
        graphClient.update(edge);
    }

    @Override
    public Graph graph() {
        return graph;
    }

    private ArangoCursor<ArangoDBVertex> getEdgeVertex(
            ArangoDBEdge edge,
            String returnStatement) {
        Map<String, Object> bindVars = new HashMap<>();
        ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
        List<String> edgeCollections = new ArrayList<>();
        edgeCollections.add(graphClient.getPrefixedCollectioName(edge.label()));
        try {
            queryBuilder.with(edgeCollections, bindVars)
                    .documentById(edge.handle(), "e", bindVars)
                    .ret(returnStatement);
        }
        catch (ArngDocument.ElementNotPairedException ex) {
            throw new ArangoDBException("Vertices for unpaired edges can't be retreived.");
        }
        String query = queryBuilder.toString();
        logger.debug("AQL {}", query);
        return graphClient.executeAqlQuery(query, bindVars, null, ArangoDBVertex.class);
    }
}
