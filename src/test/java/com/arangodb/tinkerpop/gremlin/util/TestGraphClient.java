package com.arangodb.tinkerpop.gremlin.util;

import com.arangodb.ArangoDBException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphClient;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

import java.util.Properties;

public class TestGraphClient extends ArangoDBGraphClient {

    public TestGraphClient(Properties properties, String dbname) throws ArangoDBGraphException {
        super(null, properties, dbname, false);
    }

    public void deleteGraph(String name) {
        try {
            db.graph(name).drop(true);
            db.collection(ArangoDBGraph.GRAPH_VARIABLES_COLLECTION).deleteDocument(name);
        } catch (ArangoDBException e) {
            if (e.getErrorNum() == 1924) return; // graph not found
            throw e;
        }
    }

}
