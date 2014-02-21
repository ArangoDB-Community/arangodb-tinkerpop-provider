package com.tinkerpop.blueprints.impls.arangodb.test;

import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraph;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraphException;

public class ArangoGraphTest extends ArangoDBTestCase {

	public void testCreateGraph() {
		String graph_id = null;
		try {
			ArangoDBGraph graph = new ArangoDBGraph(host, port, graphName, vertices, edges);

			assertTrue(hasGraph(graphName));

			Object x = graph.getProperty("_id");
			assertNotNull(x);

			graph_id = x.toString();
			assertFalse(graph_id.equals(""));

			graph.shutdown();
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}

		try {
			ArangoDBGraph graph = new ArangoDBGraph(host, port, graphName, vertices, edges);

			Object x = graph.getProperty("_id");
			assertNotNull(x);

			assertEquals(graph_id, x.toString());

			graph.shutdown();

		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}
	}

	public void testCreateGraph2() {
		String graph_id = null;
		try {
			ArangoDBGraph graph = new ArangoDBGraph(host, port, graphName, vertices, edges);

			assertTrue(hasGraph(graphName));

			Object x = graph.getProperty("_id");
			assertNotNull(x);

			graph_id = x.toString();
			assertFalse(graph_id.equals(""));

			graph.shutdown();
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}

		try {
			ArangoDBGraph graph = new ArangoDBGraph(host, port, graphName, null, null);

			Object x = graph.getProperty("_id");
			assertNotNull(x);

			assertEquals(graph_id, x.toString());

			graph.shutdown();

		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}
	}

}
