package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDB;
import org.apache.commons.configuration.Configuration;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

public interface ArangoDBConfiguration {

    String PROPERTY_KEY_PREFIX = "gremlin.arangodb.conf";

    String PROPERTY_KEY_DB_NAME = "graph.db";

    String PROPERTY_KEY_DB_CREATE = "graph.db.create";

    String PROPERTY_KEY_GRAPH_NAME = "graph.name";

    String PROPERTY_KEY_VERTICES = "graph.vertex";

    String PROPERTY_KEY_EDGES = "graph.edge";

    String PROPERTY_KEY_RELATIONS = "graph.relation";

    String PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES = "graph.shouldPrefixCollectionNames";

    /**
     * Get the vertex collection names defined in the configuration. Returns an empty collection if no vertex collections
     * were defined.
     *
     * Vertex collections are configured via the {@link #PROPERTY_KEY_VERTICES} setting
     *
     * @return A collection containing the defined vertex collections
     */

    Collection<String> vertexCollections();

    /**
     * Get the edge collection names defined in the configuration. Returns an empty collection if no edge collections
     * were defined.
     *
     * Edge collections are configured via the {@link #PROPERTY_KEY_EDGES} setting
     *
     * @return A collection containing the defined edge collections
     */

    Collection<String> edgeCollections();

    /**
     * Get the relations defined in the configuration. Returns an empty collection if no relations were defined.
     *
     * Relations are configured via the {@link #PROPERTY_KEY_RELATIONS} setting. If two or more vertex and two or more
     * edge collections are defined, then at least one relation must be defined too.
     *
     * @return A collection containing the defined relations
     */

    Collection<String> relations();

    /**
     * Get the graph name defined in the configuration.
     *
     * The graph name is configured via the {@link #PROPERTY_KEY_GRAPH_NAME} setting.
     *
     * @return An Optional containing the graph name, or empty if the configuration does not have the value
     */
    Optional<String> graphName();

    /**
     * Get the database name defined in the configuration.
     *
     * The database name is configured via the {@link #PROPERTY_KEY_DB_NAME} setting.
     *
     * @return An Optional containing the database name, or empty if the configuration does not have the value
     */
    Optional<String> databaseName();

    /**
     * Get the should prefix collection names flag value from the configuration. If not present the default value is true.
     *
     * The shouldPrefixCollectionNames name is configured via the {@link #PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES} setting.
     *
     * @return The flag value.
     */
    boolean shouldPrefixCollectionNames();

    /**
     * Get the create db flag value from the configuration. If not present the default value is false.
     *
     * The create db is configured via the {@link #PROPERTY_KEY_DB_CREATE} setting.
     *
     * @return The flag value.
     */

    boolean createDatabase();

    /**
     * Transform the configuration into a Properties representation
     * @return A Properties version of the configuration.
     */

    Properties transformToProperties();

    /**
     * Reurn the configuration used to create this ArangoDBConfiguration
     * @return
     */
    Configuration configuration();

    /**
     * Build a new ArangoDB (driver) using this configuration
     * @return
     */
    ArangoDB buildDriver();
}
