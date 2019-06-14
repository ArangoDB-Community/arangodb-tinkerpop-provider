package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDBException;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.GraphCreateOptions;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil.edgeDefinitionString;

public class ArngGraphClient implements GraphClient {

    private static final Logger logger = LoggerFactory.getLogger(ArngGraphClient.class);

    /** The default collection where graph variables are stored */

    public static String GRAPH_VARIABLES_COLLECTION = "TINKERPOP-GRAPH-VARIABLES";

    private final DatabaseClient db;
    private final boolean shouldPrefixCollectionNames;
    private final String graphName;
    private final boolean paired;
    private final Cache<String, ArangoDBGraphVariables> cache = CacheBuilder.newBuilder()
            .weakValues()
            .build();

    public ArngGraphClient(DatabaseClient db, String graphName, boolean shldPrfxCllctnsNms) {
        this(db, graphName, shldPrfxCllctnsNms, false);
    }

    public ArngGraphClient(
        DatabaseClient db,
        String graphName,
        boolean shouldPrefixCollectionNames,
        boolean paired) {
        this.db = db;
        this.shouldPrefixCollectionNames = shouldPrefixCollectionNames;
        this.graphName = graphName;
        this.paired = paired;
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
                            new ArangoDBQueryBuilder().insertDocument(GRAPH_VARIABLES_COLLECTION, "doc").toString(),
                            bindVars,
                            null,
                            ArangoDBGraphVariables.class));
        } catch (ArangoDBException e) {
            logger.error("Error executing AQL query to insert graph variables");
            ArangoDBGraphException arangoDBException = ArangoDBExceptions.getArangoDBException(e);
            if (arangoDBException.getErrorCode() == 1210) {
                throw Graph.Exceptions.vertexWithIdAlreadyExists(variables._key);
            }
            throw arangoDBException;
        }
        if (!it.hasNext()) {
            throw new ArangoDBGraphException("Failed to insert graph variables.");
        }
        variables = it.next().pair(this);
        cache.put("variables", variables);
        return variables;
    }

    @Override
	public ArangoDBGraphVariables getGraphVariables() throws GraphVariablesNotFoundException {
		logger.debug("Get graph variables");
        final ArngGraphClient client = this;
        try {
            return cache.get("variables", new Callable<ArangoDBGraphVariables>() {
                @Override
                public ArangoDBGraphVariables call() throws GraphVariablesNotFoundException {
                    VariableIterator it;
                    try {
                        Map<String, Object> bindVars = new HashMap<>();
                        bindVars.put("key", graphName);
                        it = new VariableIterator(client,
                        db.executeAqlQuery(
                        new ArangoDBQueryBuilder().document(GRAPH_VARIABLES_COLLECTION, "key").toString(),
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
                    result.collection(client.getPrefixedCollectioName(result.label));
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
            bindVars.put("key", variables._key());
            it = new VariableIterator(this,
                    db.executeAqlQuery(
                            new ArangoDBQueryBuilder().updateDocument(GRAPH_VARIABLES_COLLECTION, "key").toString(),
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
        variables._rev(it.next()._rev());
        cache.put("variables", variables);
        logger.info("Document updated, new rev {}", variables._rev());
    }

	@Override
	public void deleteGraphVariables(ArangoDBGraphVariables variables) {
		logger.debug("Delete variables {} in {}", variables, graphName);
		try {
            Map<String, Object> bindVars = new HashMap<>();
            bindVars.put("key", variables._key());
            new VariableIterator(this,
                db.executeAqlQuery(
                        new ArangoDBQueryBuilder().deleteDocument(GRAPH_VARIABLES_COLLECTION, "key").toString(),
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

    @Override
    public GraphClient pairWithDatabaseGraph(
        List<String> vrtxCollections,
        List<EdgeDefinition> edgeDefinitions,
        GraphCreateOptions options) throws DatabaseClient.GraphCreationException {
        ArangoGraph graph = db.graph(graphName);
        if (graph.exists()) {
            checkGraphForErrors(vrtxCollections, edgeDefinitions, graph, options);
        }
        else {
            db.createGraph(graphName, edgeDefinitions, options);
            ArangoDBGraphVariables variables = new ArangoDBGraphVariables(graphName, GRAPH_VARIABLES_COLLECTION, this);
            insertGraphVariables(variables);
        }
        return new ArngGraphClient(db, graphName, shouldPrefixCollectionNames, true);
    }

    @Override
    public String getPrefixedCollectioName(String collectionName) {
        if (GRAPH_VARIABLES_COLLECTION.equals(collectionName)) {
            return collectionName;
        }
        if(shouldPrefixCollectionNames) {
            return String.format("%s_%s", graphName, collectionName);
        }else{
            return collectionName;
        }
    }

    @Override
    public boolean isPaired() {
        return paired;
    }

    /**
     * Validate if an existing graph is correctly configured to handle the desired vertex, edges
     * and relations.
     *
     * @param verticesCollectionNames    The names of collections for nodes
     * @param configEdgeDefs                The description of edge definitions
     * @param graph                    the graph
     * @param options                    The options used to create the graph
     * @throws ArangoDBGraphException 	If the graph settings do not match the configuration information
     */

    private void checkGraphForErrors(
        List<String> verticesCollectionNames,
        List<EdgeDefinition> configEdgeDefs,
        ArangoGraph graph,
        GraphCreateOptions options) throws ArangoDBGraphException {
        if (configEdgeDefs.isEmpty()) {
            throw new IllegalArgumentException("The configuration edge definitions can not be empty.");
        }
        checkVertexCollections(verticesCollectionNames, graph, options);
        Map<String, EdgeDefinition> eds = configEdgeDefs.stream()
                .collect(Collectors.toMap(EdgeDefinition::getCollection, ed -> ed));
        Iterator<EdgeDefinition> it = graph.getInfo().getEdgeDefinitions().iterator();
        while (it.hasNext()) {
            EdgeDefinition existing = it.next();
            if (eds.containsKey(existing.getCollection())) {
                EdgeDefinition requiredEdgeDefinition = eds.remove(existing.getCollection());
                HashSet<String> existingCollections = new HashSet<>(existing.getFrom());
                HashSet<String> requiredCollections = new HashSet<>(requiredEdgeDefinition.getFrom());
                if (!existingCollections.equals(requiredCollections)) {
                    throw new ArangoDBGraphException(String.format("The 'from' collections dont match for edge definition %s", existing.getCollection()));
                }
                existingCollections.clear();
                existingCollections.addAll(existing.getTo());
                requiredCollections.clear();
                requiredCollections.addAll(requiredEdgeDefinition.getTo());
                if (!existingCollections.equals(requiredCollections)) {
                    throw new ArangoDBGraphException(String.format("The 'to' collections dont match for edge definition %s", existing.getCollection()));
                }
            } else {
                throw new ArangoDBGraphException(String.format("The graph has a surplus edge definition %s", edgeDefinitionString(existing)));
            }
        }
    }

    private void checkVertexCollections(List<String> verticesCollectionNames, ArangoGraph graph, GraphCreateOptions options) {
        List<String> allVertexCollections = new ArrayList<>(verticesCollectionNames);
        final Collection<String> orphanCollections = options.getOrphanCollections();
        if (orphanCollections != null) {
            allVertexCollections.addAll(orphanCollections);
        }
        if (!graph.getVertexCollections().containsAll(allVertexCollections)) {
            List<String> avc = new ArrayList<>(allVertexCollections);
            avc.removeAll(graph.getVertexCollections());
            throw new ArangoDBGraphException("Not all configured vertex collections appear in the graph. Missing " + avc);
        }
        if (!allVertexCollections.containsAll(graph.getVertexCollections())) {
            List<String> avc = new ArrayList<>(graph.getVertexCollections());
            avc.removeAll(allVertexCollections);
            throw new ArangoDBGraphException("Not all graph vertex collections appear in the configured vertex collections. Missing " + avc);
        }
    }

}
