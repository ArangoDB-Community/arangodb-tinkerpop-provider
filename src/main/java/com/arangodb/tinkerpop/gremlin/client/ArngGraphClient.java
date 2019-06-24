package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.EdgeEntity;
import com.arangodb.entity.VertexEntity;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.ArngGraph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A client to connect to graphs on the ArangoDB server.
 */
public class ArngGraphClient implements GraphClient {

    private static final Logger logger = LoggerFactory.getLogger(ArngGraphClient.class);

    private final DatabaseClient db;
    private final ArngGraph graph;
    private final ArngVertexClient vertexClient;

    /**
     * Create an ArngGraphClient instance that is paired with the underlying ArangoDB graph.
     *
     * @param db                    the database client to reach the db that contains the graph
     * @param graph                 the graph
     *
     */
    public ArngGraphClient(
        DatabaseClient db,
        ArngGraph graph) {
        this.db = db;
        this.graph = graph;
        this.vertexClient = new ArngVertexClient(this, graph);
    }

    @Override
    public <T> ArangoCursor<T> executeAqlQuery(
            String query,
            Map<String, Object> bindVars,
            AqlQueryOptions aqlQueryOptions,
            Class<T> type) throws ArangoDBGraphException {
        return db.executeAqlQuery(query, bindVars, aqlQueryOptions, type);
    }

    @Override
    public ArangoDBVertex insertVertex(
        String key,
        String label,
        Object... keyValues) {
        logger.debug("Insert vertex with key {} in {}", key, label);
        VertexEntity entity;
        try {
            final ArangoDBVertex dbVertex = new ArangoDBVertex(key, label);
            if (keyValues.length > 0) {
                ElementHelper.attachProperties(dbVertex, keyValues);
            }
            entity = db.graph(graph.name())
                        .vertexCollection(label)
                        .insertVertex(dbVertex);
        } catch (ArangoDBException e) {
            logger.error("Failed to insert vertex: with key {} in {}", key, label, e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
        final ArangoDBVertex arangoDBVertex = new ArangoDBVertex(
                entity.getId(),
                entity.getKey(),
                entity.getRev(),
                label,
                vertexClient);
        if (keyValues.length > 0) {
            ElementHelper.attachProperties(arangoDBVertex, keyValues);
        }
        return arangoDBVertex;
    }

    @Override
    public void remove(ArangoDBVertex vertex) {
        logger.debug("Delete vertex {} in {}", vertex, graph.name());
        try {
            db.graph(graph.name())
                    .edgeCollection(graph.getDBCollectionName(vertex.label()))
                    .deleteEdge(vertex.primaryKey());
        } catch (ArangoDBException e) {
            logger.error("Failed to delete vertex: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
    }

    @Override
    public void update(ArangoDBVertex vertex) {
        logger.debug("Update vertex {} in {}", vertex, graph.name());
        try {
            db.graph(graph.name())
                    .edgeCollection(vertex.label())
                    .updateEdge(vertex.primaryKey(), vertex);
        } catch (ArangoDBException e) {
            logger.error("Failed to update edge: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
    }

    @Override
    public ArangoDBEdge insertEdge(
        String key,
        String label,
        ArangoDBVertex from,
        ArangoDBVertex to,
        EdgeClient client,
        Object... keyValues) {
        logger.debug("Insert edge with key {} in {} ", key, label);
        EdgeEntity entity;
        try {
            ArangoDBEdge dbEdge = new ArangoDBEdge(key, label, from, to);
            if (keyValues.length > 0) {
                ElementHelper.attachProperties(dbEdge, keyValues);
            }
            entity = db.graph(graph.name())
                    .edgeCollection(graph.getDBCollectionName(label))
                    .insertEdge(dbEdge);
        } catch (ArangoDBException e) {
            logger.error("Failed to insert edge with key {} in {} ", key, label, e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
        final ArangoDBEdge arangoDBEdge = new ArangoDBEdge(
                entity.getId(),
                entity.getKey(),
                entity.getRev(),
                label,
                from,
                to,
                client);
        if (keyValues.length > 0) {
            ElementHelper.attachProperties(arangoDBEdge, keyValues);
        }
        return arangoDBEdge;
    }

    @Override
    public void remove(ArangoDBEdge edge) {
        logger.debug("Delete edge {} in {}", edge, graph.name());
		try {
			db.graph(graph.name())
			    .edgeCollection(graph.getDBCollectionName(edge.label()))
			    .deleteEdge(edge.primaryKey());
		} catch (ArangoDBException e) {
			logger.error("Failed to delete edge: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
		}
    }

    @Override
    public void update(ArangoDBEdge edge) {
        logger.debug("Update edge {} in {}", edge, graph.name());
        try {
            db.graph(graph.name())
                .edgeCollection(edge.label())
                .updateEdge(edge.primaryKey(), edge);
        } catch (ArangoDBException e) {
            logger.error("Failed to update edge: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
    }

    @Override
    public void close() throws Exception {
        db.close();
    }


}
