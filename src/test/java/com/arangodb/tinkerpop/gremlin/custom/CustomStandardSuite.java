package com.arangodb.tinkerpop.gremlin.custom;

import com.arangodb.tinkerpop.gremlin.custom.process.traversal.step.map.MergeEdgeTest;
import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;


public class CustomStandardSuite extends AbstractGremlinSuite {

    private static final Class<?>[] allTests = new Class<?>[]{
            MergeEdgeTest.Traversals.class
    };

    public CustomStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, null, false, TraversalEngine.Type.STANDARD);
    }

}
