package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoGraph;
import com.arangodb.model.GraphCreateOptions;
import org.apache.commons.configuration.Configuration;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

/**
 * This interface defines the API for working with a (Apache Commons) configuration when used to configure an #ArangoDBGraph
 *
 */
public interface GraphConfiguration {

    String PROPERTY_KEY_PREFIX = "gremlin.arangodb.conf";

    String PROPERTY_KEY_DB_NAME = "graph.db";

    String PROPERTY_KEY_DB_CREATE = "graph.db.create";

    String PROPERTY_KEY_GRAPH_NAME = "graph.name";

    String PROPERTY_KEY_VERTICES = "graph.vertex";

    String PROPERTY_KEY_EDGES = "graph.edge";

    String PROPERTY_KEY_RELATIONS = "graph.relation";

    String PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES = "graph.shouldPrefixCollectionNames";

    /**
     * Get the vertex label names defined in the configuration. Returns an empty label if no vertex collections
     * were defined.
     *
     * Vertex collections are configured via the {@link #PROPERTY_KEY_VERTICES} setting
     *
     * @return A label containing the defined vertex collections
     */

    Collection<String> vertexCollections();

    /**
     * Get the edge label names defined in the configuration. Returns an empty label if no edge collections
     * were defined.
     *
     * Edge collections are configured via the {@link #PROPERTY_KEY_EDGES} setting
     *
     * @return A label containing the defined edge collections
     */

    Collection<String> edgeCollections();

    /**
     * Get the relations defined in the configuration. Returns an empty label if no relations were defined.
     *
     * Relations are configured via the {@link #PROPERTY_KEY_RELATIONS} setting. If two or more vertex and two or more
     * edge collections are defined, then at least one relation must be defined too.
     *
     * @return A label containing the defined relations
     */

    Collection<String> relations();

    /**
     * Get the graph name defined in the configuration.
     *
     * The graph name is configured via the {@link #PROPERTY_KEY_GRAPH_NAME} setting.
     *
     * @return An Optional containing the graph name, or empty if the configuration does not have the baseValue
     */
    Optional<String> graphName();

    /**
     * Get the database name defined in the configuration.
     *
     * The database name is configured via the {@link #PROPERTY_KEY_DB_NAME} setting.
     *
     * @return An Optional containing the database name, or empty if the configuration does not have the baseValue
     */
    Optional<String> databaseName();

    /**
     * Get the should prefix label names flag baseValue from the configuration. If not present the default baseValue is true.
     *
     * The shouldPrefixCollectionNames name is configured via the {@link #PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES} setting.
     *
     * @return The flag baseValue.
     */
    boolean shouldPrefixCollectionNames();

    /**
     * Get the create db flag baseValue from the configuration. If not present the default baseValue is false.
     *
     * The create db is configured via the {@link #PROPERTY_KEY_DB_CREATE} setting.
     *
     * @return The flag baseValue.
     */

    boolean createDatabase();

    /**
     * Transform the configuration into a Properties representation
     * @return A Properties version of the configuration.
     */

    Properties transformToProperties();

    /**
     * Reurn the configuration used to create this GraphConfiguration
     * @return
     */
    Configuration configuration();

    /**
     * Build a new ArangoDB (driver) using this configuration
     * @return
     */
    ArangoDB buildDriver();

    /**
     * Return the label name correctly prefixed according to the shouldPrefixCollectionNames flag
     * @param collectionName        the label name to prefix
     * @return
     */

    /**
     * Get the label name as stored in the database
     * @param collectionName
     * @return
     */

    String getDBCollectionName(String collectionName);

    /**
     * Get the name of vertex collections for the configured graph
     * @return
     */

    Collection<String> dbVertexCollections();

    /**
     * Get the name of vertex collections for the configured graph
     * @return
     */

    Collection<String> dbEdgeCollections();

    void checkGraphForErrors(ArangoGraph databaseGraph, GraphCreateOptions options) throws ArngGraphConfiguration.MalformedRelationException;

    void createGraph(String graphName, GraphCreateOptions options);

}
