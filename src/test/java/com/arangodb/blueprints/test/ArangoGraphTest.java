package com.arangodb.blueprints.test;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.arangodb.blueprints.ArangoDBGraph;
import com.arangodb.blueprints.ArangoDBGraphException;

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
