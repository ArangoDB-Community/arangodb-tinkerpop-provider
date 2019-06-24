package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.tinkerpop.gremlin.structure.BaseArngDocument;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Iterator;

public class ArangoDBPage<EType extends BaseArngDocument> implements Iterator<EType> {

    final private String startId;
    final private int pageSize;
    final private String collection;
    final private LoadingCache<String, EType> vertices;
    final private Class<EType> eType;

    public ArangoDBPage(String startId, int pageSize, String collection) {
        this.startId = startId;
        this.collection = collection;
        this.pageSize = pageSize;

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public EType next() {
        return null;
    }


    private class Loader extends CacheLoader<String, EType> {

        final ArangoDBGraph graph;

        public Loader(ArangoDBGraph graph) {
            this.graph = graph;
        }

        @Override
        public EType load(String key) {
            String[] keyInfo = key.split("/");
            if (keyInfo.length < 2) {
                throw new ArangoDBGraphException("Element ids should consists of the label's name " +
                        "and the document primaryKey separated by /.");
            }
            assert keyInfo[1].equals(collection);
            return graph.getDatabaseClient().getElement(keyInfo[0], keyInfo[1], eType);
        }

    }
}
