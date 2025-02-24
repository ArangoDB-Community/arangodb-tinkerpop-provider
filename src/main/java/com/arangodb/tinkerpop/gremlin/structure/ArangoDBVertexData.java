package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;

public class ArangoDBVertexData extends ArangoDBData<List<ArangoDBVertexPropertyData>> {

    public ArangoDBVertexData() {
    }

    public ArangoDBVertexData(String label, String key) {
        super(label, key);
    }

    @Override
    public String toString() {
        return "ArangoDBVertexData{" +
                super.toString() +
                "}";
    }
}

