package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.entity.EdgeDefinition;

import java.util.Collection;
import java.util.List;

public interface EdgeDefinitions {

    class MalformedRelationException extends Exception {
        public MalformedRelationException(String message) {
            super(message);
        }
    }

    /**
     * Create a list of EdgeDefinitions bases on the provided configuration.
     * If the configuration has no relations, then the default edge definitions are returned.
     * Else, the corresponding edge defitions for the relations are returned.
     * @param vertexCols            the vertex collection names (prefixed)
     * @param edgeCols              the edge collection names (prefixed)
     * @param relations             the relations from the configuration
     * @return a list of EdgeDefinitions
     * @throws MalformedRelationException   if any of the relations was malformed
     */

    List<EdgeDefinition> createEdgeDefinitions(List<String> vertexCols, List<String> edgeCols, Collection<String> relations) throws MalformedRelationException;

    /**
     * Get a string representation of the Edge definition that complies with the configuration options.
     *
     * @param ed			the Edge definition
     * @return the string that represents the edge definition
     */

    String edgeDefinitionToString(EdgeDefinition ed);
}
