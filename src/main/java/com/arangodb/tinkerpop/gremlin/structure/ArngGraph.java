package com.arangodb.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Collection;
import java.util.List;

public interface ArngGraph extends Graph {

    String name();

    Collection<String> edgeCollections();

    Collection<String> vertexCollections();

    boolean hasEdgeCollection(String label);

    /**
     * Get the label name as stored in the database
     * @param collectionName
     * @return
     */

    String getDBCollectionName(String collectionName);
}
