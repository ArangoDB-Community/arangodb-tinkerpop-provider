package com.arangodb.tinkerpop.gremlin.custom;

import com.arangodb.tinkerpop.gremlin.custom.process.traversal.step.OrderabilityTest;
import com.arangodb.tinkerpop.gremlin.custom.process.traversal.step.map.MergeEdgeTest;
import com.arangodb.tinkerpop.gremlin.custom.structure.util.detached.DetachedGraphTest;
import com.arangodb.tinkerpop.gremlin.custom.structure.util.star.StarGraphTest;
import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;


public class CustomStandardSuite extends AbstractGremlinSuite {

    private static final Class<?>[] allTests = new Class<?>[]{
            MergeEdgeTest.Traversals.class,
            OrderabilityTest.Traversals.class,
            DetachedGraphTest.class,
            StarGraphTest.class
    };

    public CustomStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, null, false, TraversalEngine.Type.STANDARD);
    }

}
