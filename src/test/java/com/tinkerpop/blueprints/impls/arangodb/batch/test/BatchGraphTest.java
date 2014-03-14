package com.tinkerpop.blueprints.impls.arangodb.batch.test;

import org.junit.Test;

import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraph;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraphException;
import com.tinkerpop.blueprints.impls.arangodb.batch.ArangoDBBatchGraph;

public class BatchGraphTest extends ArangoDBBatchTestCase {

	@Test
	public void testCreateGraph() {
		String graph_id = null;
		try {
			ArangoDBBatchGraph graph = new ArangoDBBatchGraph(host, port, graphName, vertices, edges);

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
			ArangoDBBatchGraph graph = new ArangoDBBatchGraph(host, port, graphName, vertices, edges);

			Object x = graph.getProperty("_id");
			assertNotNull(x);

			assertEquals(graph_id, x.toString());

			graph.shutdown();

		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}
	}

	@Test
	public void testImport() {
		int createNum = 1000;

		try {
			ArangoDBBatchGraph graph = new ArangoDBBatchGraph(host, port, graphName, vertices, edges);

			for (Long i = 0L; i < createNum; ++i) {
				Vertex v = graph.addVertex(i);
				v.setProperty("keyA", i);
			}

			graph.shutdown();
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}

		ArangoDBGraph graph2 = null;
		try {
			graph2 = new ArangoDBGraph(host, port, graphName, vertices, edges);
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}

		GraphQuery q = graph2.query();

		assertEquals(createNum, countElements(q.vertices()));
	}

	@Test
	public void testImport2() {
		int createNum = 1000;

		try {
			ArangoDBBatchGraph graph = new ArangoDBBatchGraph(host, port, graphName, vertices, edges);

			Vertex inVertex = graph.addVertex(0L);

			for (Long i = 1L; i < createNum; ++i) {
				Vertex outVertex = graph.addVertex(i);
				graph.addEdge("Edge" + (i - 1), outVertex, inVertex, "label");
				inVertex = outVertex;
			}

			graph.addEdge("Edge" + createNum, inVertex, inVertex, "label");

			graph.shutdown();
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}

		ArangoDBGraph graph2 = null;
		try {
			graph2 = new ArangoDBGraph(host, port, graphName, vertices, edges);
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}

		GraphQuery q = graph2.query();

		assertEquals(createNum, countElements(q.vertices()));

		GraphQuery q2 = graph2.query();

		assertEquals(createNum, countElements(q2.edges()));
	}

}
