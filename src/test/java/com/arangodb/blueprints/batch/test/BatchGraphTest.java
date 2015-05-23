package com.arangodb.blueprints.batch.test;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.arangodb.blueprints.ArangoDBGraph;
import com.arangodb.blueprints.ArangoDBGraphException;
import com.arangodb.blueprints.batch.ArangoDBBatchGraph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

public class BatchGraphTest extends ArangoDBBatchTestCase {

	@Test
	public void testCreateGraph() throws ArangoDBGraphException {

		ArangoDBBatchGraph graph = new ArangoDBBatchGraph(host, port, graphName, vertices, edges);

		Assert.assertTrue(hasGraph(graphName));

		String graphId = graph.getId();
		Assert.assertNotNull(StringUtils.isNotEmpty(graphId));

		graph.shutdown();

		// reload graph and test again

		graph = new ArangoDBBatchGraph(host, port, graphName, vertices, edges);

		String x = graph.getId();
		Assert.assertNotNull(StringUtils.isNotEmpty(x));

		Assert.assertEquals(graphId, x);

		graph.shutdown();
	}

	@Test
	public void testImport() throws ArangoDBGraphException {
		int createNum = 1000;

		ArangoDBBatchGraph graph = new ArangoDBBatchGraph(host, port, graphName, vertices, edges);

		for (Long i = 0L; i < createNum; ++i) {
			Vertex v = graph.addVertex(i);
			v.setProperty("keyA", i);
		}

		graph.shutdown();

		ArangoDBGraph graph2 = new ArangoDBGraph(host, port, graphName, vertices, edges);

		GraphQuery q = graph2.query();

		Assert.assertEquals(createNum, countElements(q.vertices()));
	}

	@Test
	public void testImport2() throws ArangoDBGraphException {
		int createNum = 1000;

		ArangoDBBatchGraph graph = new ArangoDBBatchGraph(host, port, graphName, vertices, edges);

		Vertex inVertex = graph.addVertex(0L);

		for (Long i = 1L; i < createNum; ++i) {
			Vertex outVertex = graph.addVertex(i);
			graph.addEdge("Edge" + (i - 1), outVertex, inVertex, "label");
			inVertex = outVertex;
		}

		graph.addEdge("Edge" + createNum, inVertex, inVertex, "label");

		graph.shutdown();

		ArangoDBGraph graph2 = new ArangoDBGraph(host, port, graphName, vertices, edges);

		GraphQuery q = graph2.query();

		Assert.assertEquals(createNum, countElements(q.vertices()));

		GraphQuery q2 = graph2.query();

		Assert.assertEquals(createNum, countElements(q2.edges()));
	}

}
