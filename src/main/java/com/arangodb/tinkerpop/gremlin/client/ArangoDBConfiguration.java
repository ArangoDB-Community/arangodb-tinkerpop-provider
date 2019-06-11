package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDB;

import java.util.Collection;
import java.util.Optional;

public interface ArangoDBConfiguration {

    /** The properties name CONFIG_CONF. */

    static final String PROPERTY_KEY_PREFIX = "gremlin.arangodb.conf";

    /** The properties name  CONFIG_DB. */

    static final String PROPERTY_KEY_DB_NAME = "graph.db";

    /** The properties name  CONFIG_NAME. */

    static final String PROPERTY_KEY_GRAPH_NAME = "graph.name";

    /** The properties name CONFIG_VERTICES. */

    static final String PROPERTY_KEY_VERTICES = "graph.vertex";

    /** The properties name CONFIG_EDGES. */

    static final String PROPERTY_KEY_EDGES = "graph.edge";

    /** The properties name CONFIG_RELATIONS. */

    static final String PROPERTY_KEY_RELATIONS = "graph.relation";

    /** The properties name CONFIG_SHOULD_PREFIX_COLLECTION_NAMES **/

    static final String PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES = "graph.shouldPrefixCollectionNames";

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
     * Get the should prefix collection names value from the configuration. If not present the default value is true.
     *
     * The shouldPrefixCollectionNames name is configured via the {@link #PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES} setting.
     *
     * @return The flag value.
     */
    boolean shouldPrefixCollectionNames();


}
