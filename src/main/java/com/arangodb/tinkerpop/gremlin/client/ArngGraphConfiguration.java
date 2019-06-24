package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.GraphCreateOptions;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.velocipack.ArangoDBEdgeVPack;
import com.arangodb.tinkerpop.gremlin.velocipack.ArangoDBVertexVPack;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil.edgeDefinitionString;

public class PlainArangoDBConfiguration implements GraphConfiguration {

    public class MalformedRelationException extends Exception {
        public MalformedRelationException(String message) {
            super(message);
        }
    }

    private final Configuration configuration;

    public PlainArangoDBConfiguration(Configuration configuration) {
        this.configuration = configuration.subset(PROPERTY_KEY_PREFIX);
    }

    @Override
    public Collection<String> vertexCollections() {
        return configuration.getList(PROPERTY_KEY_VERTICES).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<String> edgeCollections() {
        return configuration.getList(PROPERTY_KEY_EDGES).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<String> relations() {
        return configuration.getList(PROPERTY_KEY_RELATIONS).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<String> graphName() {
        return Optional.of(configuration.getString(PROPERTY_KEY_GRAPH_NAME));
    }

    @Override
    public Optional<String> databaseName() {
        return Optional.of(configuration.getString(PROPERTY_KEY_DB_NAME));
    }

    @Override
    public boolean shouldPrefixCollectionNames() {
        return configuration.getBoolean(PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES, true);
    }

    @Override
    public Properties transformToProperties() {
        return ConfigurationConverter.getProperties(configuration);
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public ArangoDB buildDriver() {
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            transformToProperties().store(os, null);
            ByteArrayInputStream targetStream = new ByteArrayInputStream(os.toByteArray());
            ArangoDBVertexVPack vertexVpack = new ArangoDBVertexVPack();
            ArangoDBEdgeVPack edgeVPack = new ArangoDBEdgeVPack();
            return new ArangoDB.Builder().loadProperties(targetStream)
                    .registerDeserializer(ArangoDBVertex.class, vertexVpack)
                    .registerSerializer(ArangoDBVertex.class, vertexVpack)
                    .registerDeserializer(ArangoDBEdge.class, edgeVPack)
                    .registerSerializer(ArangoDBEdge.class, edgeVPack)
                    .build();
        } catch (IOException e) {
           throw new IllegalStateException("Error writing to the output stream when creating drivier.", e);
        }
    }

    @Override
    public String getDBCollectionName(String collectionName) {
        if (GraphVariablesClient.GRAPH_VARIABLES_COLLECTION.equals(collectionName)) {
            return collectionName;
        }
        if (shouldPrefixCollectionNames()) {
            return String.format("%s_%s",
                    graphName().orElseThrow(() ->
                            new IllegalStateException("Graph name property missing from configuration.")),
                    collectionName);
        } else{
            return collectionName;
        }
    }

    @Override
    public Collection<String> dbVertexCollections() {
        return vertexCollections().stream().map(this::getDBCollectionName).collect(Collectors.toList());
    }

    @Override
    public Collection<String> dbEdgeCollections() {
        return edgeCollections().stream().map(this::getDBCollectionName).collect(Collectors.toList());
    }

    @Override
    public void checkGraphForErrors(
        ArangoGraph databaseGraph,
        GraphCreateOptions options) throws MalformedRelationException {
        checkGraphForErrors(
                dbVertexCollections(),
                createEdgeDefinitions(dbVertexCollections(), dbEdgeCollections(), relations()),
                databaseGraph,
                options);
    }

    @Override
    public void createGraph(String graphName, GraphCreateOptions options) {

    }

    @Override
    public boolean createDatabase() {
        return configuration.getBoolean(PROPERTY_KEY_DB_CREATE, false);
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
        Collection<String> verticesCollectionNames,
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

    private void checkVertexCollections(
        Collection<String> vertexCollections,
        ArangoGraph graph,
        GraphCreateOptions options) {
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

    private List<EdgeDefinition> createEdgeDefinitions(
        Collection<String> vertexCols,
        Collection<String> edgeCols,
        Collection<String> relations) throws MalformedRelationException {
        final List<EdgeDefinition> edgeDefinitions;
        if (relations.isEmpty()) {
            // logger.info("No relations, creating default ones.");
            edgeDefinitions = createDefaultEdgeDefinitions(vertexCols, edgeCols);
        } else {
            edgeDefinitions = new ArrayList<>();
            for (String value : relations) {
                edgeDefinitions.add(relationPropertyToEdgeDefinition(value));
            }
        }
        return edgeDefinitions;
    }

    /**
     * Creates the default edge definitions. When no relations are provided, the graph schema is
     * assumed to be fully connected, i.e. there is an EdgeDefintion for each possible combination
     * of Vertex-Edge-Vertex triplets.
     *
     * @param verticesCollectionNames 	the vertex label names
     * @param edgesCollectionNames 		the edge label names
     * @return the list of edge definitions
     */

    private List<EdgeDefinition> createDefaultEdgeDefinitions(
            Collection<String> verticesCollectionNames,
            Collection<String> edgesCollectionNames) {
        List<EdgeDefinition> result = new ArrayList<>();
        for (String e : edgesCollectionNames) {
            for (String from : verticesCollectionNames) {
                for (String to : verticesCollectionNames) {
                    EdgeDefinition ed = new EdgeDefinition()
                            .collection(e)
                            .from(from)
                            .to(to);
                    result.add(ed);
                }
            }
        }
        return result;
    }

    /**
     * Create an EdgeDefinition from a relation in the Configuration. The format of a relation is:
     * <pre>
     * label:from-&gt;to
     * </pre>
     * Where label is the name of the Edge label, and to and from are comma separated list of
     * node label names.
     *
     * @param relation 				the relation
     * @return an EdgeDefinition that represents the relation.
     * @throws ArangoDBGraphException if the relation is malformed
     */

    private EdgeDefinition relationPropertyToEdgeDefinition(String relation) throws MalformedRelationException {
        // logger.info("Creating EdgeRelation from {}", relation);
        EdgeDefinition result = new EdgeDefinition();
        String[] info = relation.split(":");
        if (info.length != 2) {
            throw new MalformedRelationException("Error in configuration. Malformed relation " + relation);
        }
        result.collection(getDBCollectionName(info[0]));
        info = info[1].split("->");
        if (info.length != 2) {
            throw new MalformedRelationException("Error in configuration. Malformed relation> " + relation);
        }
        List<String> trimmed = Arrays.stream(info[0].split(","))
                .map(String::trim)
                .map(c -> getDBCollectionName(c))
                .collect(Collectors.toList());
        String[] from = new String[trimmed.size()];
        from = trimmed.toArray(from);

        trimmed = Arrays.stream(info[1].split(","))
                .map(String::trim)
                .map(c -> getDBCollectionName(c))
                .collect(Collectors.toList());
        String[] to = new String[trimmed.size()];
        to = trimmed.toArray(to);
        result.from(from).to(to);
        return result;
    }
}
