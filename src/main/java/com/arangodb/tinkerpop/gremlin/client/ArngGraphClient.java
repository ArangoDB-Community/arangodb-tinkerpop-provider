package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDBException;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.DocumentEntity;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.GraphCreateOptions;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
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

/**
 * A client to connect to graphs on the ArangoDB server.
 */
public class ArngGraphClient implements GraphClient {

    private static final Logger logger = LoggerFactory.getLogger(ArngGraphClient.class);

    private final DatabaseClient db;
    private final boolean prefixCollectionNames;
    private final String graphName;
    private final boolean paired;

    /**
     * Create an ArngGraphClient instance that is not paired with the underlying ArangoDB graph. After construction
     * {@link #pairWithDatabaseGraph(List, List, GraphCreateOptions)} should be invoked to get a paired instance.
     * @param db                    the database client to reach the db that contains the graph
     * @param graphName             the graph name
     * @param prfxCllctnsNms        a boolean flag to indicate if graph collections are prefixed or not
     *
     * @see #pairWithDatabaseGraph(List, List, GraphCreateOptions)
     */
    public ArngGraphClient(DatabaseClient db, String graphName, boolean prfxCllctnsNms) {
        this(db, graphName, prfxCllctnsNms, false);
    }

    /**
     * Create an ArngGraphClient instance that is paired with the underlying ArangoDB graph. This method is private
     * as {@link #pairWithDatabaseGraph(List, List, GraphCreateOptions)} is the only way to safely pair this client
     * to the underlying graph as it does error check or creation correctly.
     *
     * @param db                    the database client to reach the db that contains the graph
     * @param graphName             the graph name
     * @param shldPrfxCllctnsNms    a boolean flag to indicate if graph collections are prefixed or not
     * @param paired                a boolean flag to indicate if the client is paired
     *
     * @see #pairWithDatabaseGraph(List, List, GraphCreateOptions)
     */
    private ArngGraphClient(
        DatabaseClient db,
        String graphName,
        boolean shldPrfxCllctnsNms,
        boolean paired) {
        this.db = db;
        this.prefixCollectionNames = shldPrfxCllctnsNms;
        this.graphName = graphName;
        this.paired = paired;
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
        }
        return new ArngGraphClient(db, graphName, prefixCollectionNames, true);
    }

    @Override
    public String getPrefixedCollectioName(String collectionName) {
        if (GraphVariablesClient.GRAPH_VARIABLES_COLLECTION.equals(collectionName)) {
            return collectionName;
        }
        if(prefixCollectionNames) {
            return String.format("%s_%s", graphName, collectionName);
        }else{
            return collectionName;
        }
    }

    @Override
    public boolean isPaired() {
        return paired;
    }

    @Override
    public ArangoDBVertex insertVertex(ArangoDBVertex vertex) {
        logger.debug("Insert document {} in {}", vertex, graphName);
		if (vertex.isPaired()) {
			throw new ArangoDBGraphException("Paired docuemnts can not be inserted, only updated");
		}
        VertexIterator it;
        try {
            Map<String, Object> bindVars = new HashMap<>();
            bindVars.put("doc", vertex);
            it = new VertexIterator(
                    this,
                    db.executeAqlQuery(
                            new ArangoDBQueryBuilder().insertDocument(vertex.collection, "doc").toString(),
                            bindVars,
                            null,
                            ArangoDBVertex.class));
        } catch (ArangoDBException e) {
            logger.error("Error executing AQL query to insert graph variables");
            ArangoDBGraphException arangoDBException = ArangoDBExceptions.getArangoDBException(e);
            if (arangoDBException.getErrorCode() == 1210) {
                throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex._key);
            }
            throw arangoDBException;
        }
        if (!it.hasNext()) {
            throw new ArangoDBGraphException("Failed to insert graph variables.");
        }
        return new ArangoDBVertex()
        vertex._id(documentEntity.getId());
        vertex._rev(documentEntity.getRev());
		if (vertex._key() == null) {
            vertex._key(documentEntity.getKey());
		}
        vertex.setPaired(true);
    }

    @Override
    public void close() throws Exception {
        db.close();
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

    /**
     * Check that the desired vertex collections match the vertex collections used by the graph.
     * @param vertexCollections     the names of the vertex collections
     * @param graph                 the graph
     * @param options               the graph options
     */

    private void checkVertexCollections(List<String> vertexCollections, ArangoGraph graph, GraphCreateOptions options) {
        List<String> allVertexCollections = new ArrayList<>(vertexCollections);
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
