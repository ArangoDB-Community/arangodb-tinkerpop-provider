package com.arangodb.tinkerpop.gremlin.velocipack;

import com.arangodb.tinkerpop.gremlin.structure.ArngDocument;
import com.arangodb.tinkerpop.gremlin.structure.ArngEdge;
import com.arangodb.velocypack.VPackBuilder;
import com.arangodb.velocypack.ValueType;

public class VPackElement implements AutoCloseable {

    private final VPackBuilder builder;

    public VPackElement(VPackBuilder builder) {
        this.builder = builder;
    }

    @Override
    public void close() throws Exception {
        builder.close();
    }

    public void startObject(String name) {
        builder.add(name, ValueType.OBJECT);
    }


    public void addSpecialAttributes(ArngDocument value) {
        try {
            builder.add("_id", value.handle());
        } catch (ArngDocument.ElementNotPairedException e) {
            // Pass, simply dont provide id
        }
        if (value.primaryKey() != null) {
            builder.add("_key", value.primaryKey());
        }
        try {
            builder.add("_rev", value.revision());
        } catch (ArngDocument.ElementNotPairedException e) {
            // Pass, simply dont provide rev
        }
    }

    public void addEdgeAttributes(ArngEdge value) {
        addSpecialAttributes(value);
        builder.add("_from", value.from());
        builder.add("_to", value.to());
    }
}
