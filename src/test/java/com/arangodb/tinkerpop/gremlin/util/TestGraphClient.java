package com.arangodb.tinkerpop.gremlin.util;

import com.arangodb.ArangoDBException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphClient;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

import java.util.Properties;

public class TestGraphClient extends ArangoDBGraphClient {

    public TestGraphClient(Properties properties, String dbname) throws ArangoDBGraphException {
        super(null, properties, dbname);
        if (!db.exists()) {
            if (!db.create()) {
                throw new ArangoDBGraphException("Unable to crate the database " + dbname);
            }
        }
    }

    public void clear(String name) {
        try {
            db.collection(ArangoDBGraph.GRAPH_VARIABLES_COLLECTION).deleteDocument(name);
        } catch (ArangoDBException e) {
            if (e.getErrorNum() != 1202         // document not found
                    && e.getErrorNum() != 1203  // collection not found
            ) throw e;
        }

        try {
            db.graph(name).drop(true);
        } catch (ArangoDBException e) {
            if (e.getErrorNum() != 1924) // graph not found
                throw e;
        }

        db.clearQueryCache();
    }

}
