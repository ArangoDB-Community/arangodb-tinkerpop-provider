package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class EssentialArangoGraph implements ArangoGraph {

    private static final Logger logger = LoggerFactory.getLogger(EssentialArangoGraph.class);
    private final Database db;

    public EssentialArangoGraph(Database db) {
        this.db = db;
    }

    @Override
    public ArangoDBGraphVariables insertGraphVariables(
            ArangoDBGraphVariables variables,
            ArangoDBGraph graph) {
        logger.debug("Insert graph variables {} in {}", variables, graph.name());
        if (variables.isPaired()) {
            throw new ArangoDBGraphException("Paired documents can not be inserted, only updated");
        }
        ArangoDBIterator<ArangoDBGraphVariables> it;
        try {
            Map<String, Object> bindVars = new HashMap<>();
            bindVars.put("doc", variables);
            it = new ArangoDBIterator<>(graph,
                    db.executeAqlQuery(
                            new ArangoDBQueryBuilder().insertDocument(graph.GRAPH_VARIABLES_COLLECTION, "doc").toString(),
                            bindVars,
                            null,
                            ArangoDBGraphVariables.class));
        } catch (ArangoDBException e) {
            logger.error("Failed to insert document: {}", e.getMessage());
            ArangoDBGraphException arangoDBException = ArangoDBExceptions.getArangoDBException(e);
            if (arangoDBException.getErrorCode() == 1210) {
                throw Graph.Exceptions.vertexWithIdAlreadyExists(variables._key);
            }
            throw arangoDBException;
        }
        if (!it.hasNext()) {
            throw new ArangoDBGraphException("Failed to insert graph variables.");
        }
        return it.next();
    }

    @Override
	public ArangoDBGraphVariables getGraphVariables(ArangoDBGraph graph) {
		logger.debug("Get graph variables");
        ArangoDBIterator<ArangoDBGraphVariables> it;
		try {
            Map<String, Object> bindVars = new HashMap<>();
            bindVars.put("key", graph.name());
            it = new ArangoDBIterator<>(graph,
                    db.executeAqlQuery(
                        new ArangoDBQueryBuilder().document(graph.GRAPH_VARIABLES_COLLECTION, "key").toString(),
                        bindVars,
                        null,
                        ArangoDBGraphVariables.class));
		} catch (ArangoDBException e) {
			logger.error("Failed to retrieve vertex: {}", e.getErrorMessage());
			throw new ArangoDBGraphException("Failed to retrieve vertex.", e);
		}
		if (!it.hasNext()) {
            return null;
        }
        ArangoDBGraphVariables result = it.next();
		result.collection(result.label);
		return result;
	}

    @Override
    public ArangoDBGraphVariables updateGraphVariables(
            ArangoDBGraphVariables variables,
            ArangoDBGraph graph) {
        logger.debug("Update variables {} in {}", variables, graph.name());
        ArangoDBIterator<ArangoDBGraphVariables> it;
        try {
            Map<String, Object> bindVars = new HashMap<>();
            bindVars.put("key", variables._key());
            it = new ArangoDBIterator<>(graph,
                    db.executeAqlQuery(
                            new ArangoDBQueryBuilder().updateDocument(graph.GRAPH_VARIABLES_COLLECTION, "key").toString(),
                            bindVars,
                            null,
                            ArangoDBGraphVariables.class));
        } catch (ArangoDBException e) {
            logger.error("Failed to update document: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
        if (!it.hasNext()) {
            throw new ArangoDBGraphException("Failed to update graph variables.");
        }
        ArangoDBGraphVariables result = it.next();
        logger.info("Document updated, new rev {}", result._rev());
        return result;
    }

	@Override
	public void deleteGraphVariables(
            ArangoDBGraphVariables variables,
            ArangoDBGraph graph) {
		logger.debug("Delete variables {} in {}", variables, graph.name());
        ArangoDBIterator<ArangoDBGraphVariables> it;
		try {
            Map<String, Object> bindVars = new HashMap<>();
            bindVars.put("key", variables._key());
            it = new ArangoDBIterator<>(graph,
                    db.executeAqlQuery(
                            new ArangoDBQueryBuilder().deleteDocument(graph.GRAPH_VARIABLES_COLLECTION, "key").toString(),
                            bindVars,
                            null,
                            ArangoDBGraphVariables.class));
		} catch (ArangoDBException e) {
			logger.error("Failed to delete document: {}", e.getErrorMessage());
			throw ArangoDBExceptions.getArangoDBException(e);
		}
		variables.setPaired(false);
	}






}
