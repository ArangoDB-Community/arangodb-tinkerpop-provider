package com.arangodb.tinkerpop.gremlin.custom;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;


public class CustomStandardSuite extends AbstractGremlinSuite {

    private static final Class<?>[] allTests = new Class<?>[]{
    };

    private static final Class<?>[] testsToEnforce = new Class<?>[]{
    };

    public CustomStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, testsToEnforce, false, TraversalEngine.Type.STANDARD);
    }

}
