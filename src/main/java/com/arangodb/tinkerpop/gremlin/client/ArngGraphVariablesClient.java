package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDBException;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class ArngGraphVariablesClient implements GraphVariablesClient {

    private static final Logger logger = LoggerFactory.getLogger(ArngGraphVariablesClient.class);

    private final DatabaseClient db;
    private final String graphName;
    private final Cache<String, ArangoDBGraphVariables> cache = CacheBuilder.newBuilder()
            .weakValues()
            .build();

    public ArngGraphVariablesClient(DatabaseClient db, String graphName) {
        this.db = db;
        this.graphName = graphName;
    }

    @Override
    public String graphName() {
        return graphName;
    }

    @Override
    public ArangoDBGraphVariables insertGraphVariables(ArangoDBGraphVariables variables) {
        logger.debug("Insert graph variables {} in {}", variables, graphName);
        if (variables.isPaired()) {
            throw new ArangoDBGraphException("Paired documents can not be inserted, only updated");
        }
        VariableIterator it;
        try {
            Map<String, Object> bindVars = new HashMap<>();
            bindVars.put("doc", variables);
            it = new VariableIterator(
                    this,
                    db.executeAqlQuery(
                            new ArangoDBQueryBuilder().insertDocument(GraphVariablesClient.GRAPH_VARIABLES_COLLECTION, "doc").toString(),
                            bindVars,
                            null,
                            ArangoDBGraphVariables.class));
        } catch (ArangoDBException e) {
            logger.error("Error executing AQL query to insertEdge graph variables");
            ArangoDBGraphException arangoDBException = ArangoDBExceptions.getArangoDBException(e);
            if (arangoDBException.getErrorCode() == 1210) {
                throw Graph.Exceptions.vertexWithIdAlreadyExists(variables._key);
            }
            throw arangoDBException;
        }
        if (!it.hasNext()) {
            throw new ArangoDBGraphException("Failed to insertEdge graph variables.");
        }
        cache.put("variables", variables);
        return variables;
    }

    @Override
    public ArangoDBGraphVariables getGraphVariables() throws GraphVariablesNotFoundException {
        logger.debug("Get graph variables");
        final GraphVariablesClient client = this;
        try {
            return cache.get("variables", new Callable<ArangoDBGraphVariables>() {
                @Override
                public ArangoDBGraphVariables call() throws GraphVariablesNotFoundException {
                    VariableIterator it;
                    try {
                        Map<String, Object> bindVars = new HashMap<>();
                        bindVars.put("primaryKey", graphName);
                        it = new VariableIterator(client,
                                db.executeAqlQuery(
                                        new ArangoDBQueryBuilder().document(GraphVariablesClient.GRAPH_VARIABLES_COLLECTION, "primaryKey").toString(),
                                        bindVars,
                                        null,
                                        ArangoDBGraphVariables.class));
                    } catch (ArangoDBException e) {
                        logger.error("Error executing AQL query to get graph variables");
                        throw ArangoDBExceptions.getArangoDBException(e);
                    }
                    if (!it.hasNext()) {
                        throw new GraphVariablesNotFoundException(String.format("No graph variables found for graph %s", graphName));
                    }
                    ArangoDBGraphVariables result = it.next();
                    result.label(GraphVariablesClient.GRAPH_VARIABLES_COLLECTION);
                    return result;
                }
            });
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(
                    e.getCause(), GraphVariablesNotFoundException.class);
            throw new IllegalStateException(e);
        } catch (UncheckedExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void updateGraphVariables(ArangoDBGraphVariables variables) throws GraphVariablesNotFoundException {
        logger.debug("Update variables {} in {}", variables.toString(), graphName);
        if (!variables.isPaired()) {
            throw new ArangoDBGraphException("Unpaired variables can not be updated, only inserted");
        }
        VariableIterator it;
        try {
            Map<String, Object> bindVars = new HashMap<>();
            bindVars.put("primaryKey", variables.primaryKey());
            it = new VariableIterator(this,
                    db.executeAqlQuery(
                            new ArangoDBQueryBuilder().updateDocument(GraphVariablesClient.GRAPH_VARIABLES_COLLECTION, "primaryKey").toString(),
                            bindVars,
                            null,
                            ArangoDBGraphVariables.class));
        } catch (ArangoDBException e) {
            logger.error("Error executing AQL query to update graph variables");
            throw ArangoDBExceptions.getArangoDBException(e);
        }
        if (!it.hasNext()) {
            throw new GraphVariablesNotFoundException("Failed to update graph variables.");
        }
        variables.revision(it.next().revision());
        cache.put("variables", variables);
        logger.info("ArngDocument updated, new revision {}", variables.revision());
    }

    @Override
    public void deleteGraphVariables(ArangoDBGraphVariables variables) {
        logger.debug("Delete variables {} in {}", variables, graphName);
        try {
            Map<String, Object> bindVars = new HashMap<>();
            bindVars.put("primaryKey", variables.primaryKey());
            new VariableIterator(this,
                    db.executeAqlQuery(
                            new ArangoDBQueryBuilder().deleteDocument(GraphVariablesClient.GRAPH_VARIABLES_COLLECTION, "primaryKey").toString(),
                            bindVars,
                            null,
                            ArangoDBGraphVariables.class));
        } catch (ArangoDBException e) {
            logger.error("Error executing AQL query to delete graph variables");
            throw ArangoDBExceptions.getArangoDBException(e);
        }
        variables.setPaired(false);
        cache.invalidate("variables");
    }
}
