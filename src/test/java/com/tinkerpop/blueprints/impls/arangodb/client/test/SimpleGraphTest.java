package com.tinkerpop.blueprints.impls.arangodb.client.test;

import org.junit.Assert;
import org.junit.Test;

import com.arangodb.ArangoException;
import com.arangodb.ErrorNums;
import com.arangodb.entity.EdgeDefinitionEntity;
import com.arangodb.entity.GraphEntity;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleGraph;

public class SimpleGraphTest extends BaseTestCase {

	@Test
	public void test_CreateSimpleGraph() throws ArangoException {

		ArangoDBSimpleGraph graph = client.createGraph(graphName, vertices, edges);

		Assert.assertNotNull(graph);
		Assert.assertEquals(graphName, graph.getName());
		Assert.assertEquals(vertices, graph.getVertexCollection());
		Assert.assertEquals(edges, graph.getEdgeCollection());
	}

	@Test
	public void test_GetSimpleGraph() throws ArangoException {

		ArangoDBSimpleGraph graph = client.createGraph(graphName, vertices, edges);

		Assert.assertNotNull(graph);
		Assert.assertEquals(graphName, graph.getName());
		Assert.assertEquals(vertices, graph.getVertexCollection());
		Assert.assertEquals(edges, graph.getEdgeCollection());

		GraphEntity graph2 = client.getGraph(graphName);
		Assert.assertNotNull(graph2);

		Assert.assertEquals(graphName, graph2.getName());
		Assert.assertEquals(1, graph2.getEdgeDefinitions().size());
		EdgeDefinitionEntity edgeDefinitionEntity = graph2.getEdgeDefinitions().get(0);
		Assert.assertEquals(1, edgeDefinitionEntity.getTo().size());
		Assert.assertEquals(1, edgeDefinitionEntity.getFrom().size());
		Assert.assertEquals(vertices, edgeDefinitionEntity.getFrom().get(0));
		Assert.assertEquals(vertices, edgeDefinitionEntity.getTo().get(0));
		Assert.assertEquals(edges, edgeDefinitionEntity.getCollection());

	}

	@Test
	public void test_DeleteSimpleGraph() throws ArangoException {

		ArangoDBSimpleGraph graph = client.createGraph(graphName, vertices, edges);

		boolean b = client.deleteGraph(graph.getGraphEntity());
		Assert.assertTrue(b);

		GraphEntity graph2 = client.getGraph(graphName);
		Assert.assertNull(graph2);
	}

	@Test
	public void test_ReCreateGraph() throws ArangoException {

		ArangoDBSimpleGraph graph = client.createGraph(graphName, vertices, edges);

		Assert.assertNotNull(graph);
		Assert.assertEquals(graphName, graph.getName());
		Assert.assertEquals(vertices, graph.getVertexCollection());
		Assert.assertEquals(edges, graph.getEdgeCollection());

		try {
			client.createGraph(graphName, vertices, edges);
		} catch (ArangoException e) {
			Assert.assertEquals(ErrorNums.ERROR_GRAPH_DUPLICATE, e.getErrorNumber());
		}

	}

}
