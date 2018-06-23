package com.arangodb.tinkerpop.gremlin.test;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class ArangoGraphTest extends ArangoDBTestCase {

	@Test
	public void testCreateGraph() throws ArangoDBGraphException {
		ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);

		assertTrue(hasGraph(graphName));

		String graphId = graph.getId();
		assertNotNull(StringUtils.isNotEmpty(graphId));

		graph.shutdown();

		graph = new ArangoDBGraph(configuration, graphName, vertices, edges);

		String x = graph.getId();
		assertNotNull(StringUtils.isNotEmpty(x));

		assertEquals(graphId, x);

		graph.shutdown();
	}

	@Test
	public void testCreateGraphFail() {
		try {
			new ArangoDBGraph(configuration, graphName, null, null);
			Assert.fail("This should throw an exception");
		} catch (ArangoDBGraphException e) {

		}
	}

}
