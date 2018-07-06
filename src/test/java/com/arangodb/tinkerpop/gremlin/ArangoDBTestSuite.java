package com.arangodb.tinkerpop.gremlin;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest;
import org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest;
import org.apache.tinkerpop.gremlin.structure.EdgeTest;
import org.apache.tinkerpop.gremlin.structure.FeatureSupportTest;
import org.apache.tinkerpop.gremlin.structure.GraphConstructionTest;
import org.apache.tinkerpop.gremlin.structure.GraphTest;
import org.apache.tinkerpop.gremlin.structure.PropertyTest;
import org.apache.tinkerpop.gremlin.structure.SerializationTest;
import org.apache.tinkerpop.gremlin.structure.TransactionTest;
import org.apache.tinkerpop.gremlin.structure.VariablesTest;
import org.apache.tinkerpop.gremlin.structure.VertexPropertyTest;
import org.apache.tinkerpop.gremlin.structure.io.IoCustomTest;
import org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest;
import org.apache.tinkerpop.gremlin.structure.io.IoGraphTest;
import org.apache.tinkerpop.gremlin.structure.io.IoPropertyTest;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.IoVertexTest;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdgeTest;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedGraphTest;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPropertyTest;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexPropertyTest;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexTest;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdgeTest;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceGraphTest;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexPropertyTest;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexTest;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBStructureCheck;

/**
 * Run with {@code GREMLIN_TESTS} environment variable set to a list of any of these to enable
 * particular tests:
 * org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest,
 * org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest,
 * org.apache.tinkerpop.gremlin.structure.EdgeTest,
 * org.apache.tinkerpop.gremlin.structure.FeatureSupportTest,
 * org.apache.tinkerpop.gremlin.structure.io.IoCustomTest,
 * org.apache.tinkerpop.gremlin.structure.io.IoGraphTest,
 * org.apache.tinkerpop.gremlin.structure.io.IoVertexTest,
 * org.apache.tinkerpop.gremlin.structure.io.IoPropertyTest,
 * org.apache.tinkerpop.gremlin.structure.GraphTest,
 * org.apache.tinkerpop.gremlin.structure.GraphConstructionTest,
 * org.apache.tinkerpop.gremlin.structure.io.IoTest,
 * org.apache.tinkerpop.gremlin.structure.VertexPropertyTest
 * 
 */
public class ArangoDBTestSuite extends AbstractGremlinSuite {
	
	/**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[]{
    	CommunityGeneratorTest.class,
    	// Missing
        DetachedGraphTest.class,
        DetachedEdgeTest.class,
        DetachedVertexPropertyTest.class,
        DetachedPropertyTest.class,
        DetachedVertexTest.class,
        // Missing
        
        DistributionGeneratorTest.class,
        EdgeTest.class,
        FeatureSupportTest.class,
        IoCustomTest.class,
        IoEdgeTest.class,		
        IoGraphTest.class,
        IoVertexTest.class,
        IoPropertyTest.class,
        GraphTest.class,
        GraphConstructionTest.class,
        IoTest.class,
        VertexPropertyTest.class,
        VariablesTest.class,
        PropertyTest.class,
        ReferenceGraphTest.class,
        ReferenceEdgeTest.class,
        ReferenceVertexPropertyTest.class,
        ReferenceVertexTest.class,
        SerializationTest.class,
        StarGraphTest.class,
        TransactionTest.class,
        VertexTest.class,
    	ArangoDBStructureCheck.class,
         //ArangoDBIndexCheck.class,
         //ArangoDBCypherCheck.class,
    };

	public ArangoDBTestSuite(
		Class<?> klass,
		RunnerBuilder builder)
		throws InitializationError {
		super(klass, builder, allTests, null, false, TraversalEngine.Type.STANDARD);
	}

}
