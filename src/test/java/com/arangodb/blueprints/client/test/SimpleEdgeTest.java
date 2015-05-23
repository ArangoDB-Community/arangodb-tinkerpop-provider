package com.arangodb.blueprints.client.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.arangodb.ArangoException;
import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.client.ArangoDBSimpleEdge;
import com.arangodb.blueprints.client.ArangoDBSimpleGraph;
import com.arangodb.blueprints.client.ArangoDBSimpleVertex;

public class SimpleEdgeTest extends BaseTestCase {

	ArangoDBSimpleGraph graph = null;
	ArangoDBSimpleVertex vertex1 = null;
	ArangoDBSimpleVertex vertex2 = null;
	ArangoDBSimpleVertex vertex3 = null;
	ArangoDBSimpleVertex vertex4 = null;
	ArangoDBSimpleVertex vertex5 = null;

	@Before
	public void setUp() {
		super.setUp();
		try {
			graph = client.createGraph(graphName, vertices, edges);
			vertex1 = client.createVertex(graph, "v1", null);
			vertex2 = client.createVertex(graph, "v2", null);
			vertex3 = client.createVertex(graph, "v3", null);
			vertex4 = client.createVertex(graph, "v4", null);
			vertex5 = client.createVertex(graph, "v5", null);

		} catch (ArangoException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		} catch (ArangoDBException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}
	}

	@After
	public void tearDown() {
		super.tearDown();
	}

	@Test
	public void test_CreateSimpleEdge() throws ArangoDBException {
		ArangoDBSimpleEdge edge = client.createEdge(graph, null, null, vertex1, vertex2, null);
		Assert.assertNotNull(edge);
		Assert.assertNotNull(edge.getDocumentKey());
	}

	@Test
	public void test_CreateSimpleEdgeWithLabel() throws ArangoDBException {
		ArangoDBSimpleEdge edge = client.createEdge(graph, "edge1", "label1", vertex1, vertex2, null);
		Assert.assertNotNull(edge);
		Assert.assertNotNull(edge.getDocumentKey());
		Assert.assertEquals("edge1", edge.getName());
		Assert.assertEquals("label1", edge.getLabel());

	}

	@Test
	public void test_CreateSimpleEdgeWithProperties() throws ArangoDBException {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("hello", "world");
		properties.put("key", "value");

		ArangoDBSimpleEdge edge = client.createEdge(graph, "edge1", "label1", vertex1, vertex2, properties);

		Assert.assertNotNull(edge);
		Assert.assertNotNull(edge.getDocumentKey());
		Assert.assertEquals("edge1", edge.getName());
		Assert.assertEquals("label1", edge.getLabel());
		Assert.assertEquals("world", edge.getProperty("hello"));
		Assert.assertEquals("value", edge.getProperty("key"));
	}

	@Test
	public void test_GetSimpleEdge() throws ArangoDBException {

		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("hello", "world");
		properties.put("key", "value");

		client.createEdge(graph, "edge1", "label1", vertex1, vertex2, properties);

		ArangoDBSimpleEdge edge2 = client.getEdge(graph, "edge1");

		Assert.assertNotNull(edge2);
		Assert.assertNotNull(edge2.getDocumentKey());
		Assert.assertEquals("edge1", edge2.getName());
		Assert.assertEquals("label1", edge2.getLabel());
		Assert.assertEquals("world", edge2.getProperty("hello"));
		Assert.assertEquals("value", edge2.getProperty("key"));
	}

	@Test
	public void test_ChangeSimpleEdge() throws ArangoDBException {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("name", "mayer");
		properties.put("street", "barmer");

		client.createEdge(graph, "edge1", "label1", vertex1, vertex2, properties);

		ArangoDBSimpleEdge edge2 = client.getEdge(graph, "edge1");

		Assert.assertNotNull(edge2);
		Assert.assertNotNull(edge2.getDocumentKey());
		Assert.assertEquals("mayer", edge2.getProperty("name"));
		Assert.assertEquals("barmer", edge2.getProperty("street"));

		edge2.removeProperty("street");
		edge2.setProperty("name", "mueller");

		client.saveEdge(graph, edge2);
		Assert.assertEquals("mueller", edge2.getProperty("name"));
		Assert.assertNull(edge2.getProperty("street"));

		ArangoDBSimpleEdge edge3 = client.getEdge(graph, "edge1");

		Assert.assertNotNull(edge3);
		Assert.assertNotNull(edge3.getDocumentKey());
		Assert.assertEquals("mueller", edge3.getProperty("name"));
		Assert.assertNull(edge3.getProperty("street"));
	}

	@Test
	public void test_DeleteSimpleEdge() throws ArangoDBException {
		ArangoDBSimpleEdge edge = client.createEdge(graph, null, null, vertex1, vertex2, null);

		Assert.assertNotNull(edge);
		Assert.assertNotNull(edge.getDocumentKey());

		String key = edge.getDocumentKey();

		boolean b = client.deleteEdge(graph, edge);
		Assert.assertTrue(b);

		try {
			ArangoDBSimpleEdge edge2 = client.getEdge(graph, key);
			Assert.assertNull(edge2);
			Assert.assertTrue(false);
		} catch (ArangoDBException e) {
			Assert.assertTrue(true);
		}
	}

}
