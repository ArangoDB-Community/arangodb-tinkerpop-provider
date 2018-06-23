package com.arangodb.tinkerpop.gremlin.client.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.arangodb.ArangoException;
import com.arangodb.ErrorNums;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBSimpleGraph;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBSimpleVertex;

public class SimpleVertexTest extends BaseTestCase {

	ArangoDBSimpleGraph graph = null;

	@Before
	public void setUp() {
		super.setUp();
		try {
			graph = client.createGraph(graphName, vertices, edges);
		} catch (ArangoException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}
	}

	@Test
	public void test_CreateSimpleVertex() throws ArangoDBException {
		ArangoDBSimpleVertex vertex = client.createVertex(graph, null, null);
		Assert.assertNotNull(vertex);
		Assert.assertNotNull(vertex.getDocumentKey());
	}

	@Test
	public void test_CreateAndGetSimpleVertex() throws ArangoDBException {
		ArangoDBSimpleVertex vertex = client.createVertex(graph, null, null);
		Assert.assertNotNull(vertex);
		Assert.assertNotNull(vertex.getDocumentKey());

		ArangoDBSimpleVertex vertex2 = client.getVertex(graph, vertex.getDocumentKey());
		Assert.assertNotNull(vertex2);
		Assert.assertNotNull(vertex2.getDocumentKey());

		ArangoDBSimpleVertex vertex3 = client.getVertex(graph, vertex.getDocumentKey());
		Assert.assertNotNull(vertex3);
		Assert.assertNotNull(vertex3.getDocumentKey());
	}

	@Test
	public void test_CreateSimpleVertexWithName() throws ArangoDBException {
		ArangoDBSimpleVertex vertex = client.createVertex(graph, "egon", null);

		Assert.assertNotNull(vertex);
		Assert.assertNotNull(vertex.getDocumentKey());
		Assert.assertEquals("egon", vertex.getDocumentKey());
	}

	@Test
	public void test_CreateSimpleVertexWithProperties() throws ArangoDBException {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("fisch", "mehl");
		properties.put("counter", 5);

		ArangoDBSimpleVertex vertex = client.createVertex(graph, "egon", properties);

		Assert.assertNotNull(vertex);
		Assert.assertNotNull(vertex.getDocumentKey());
		Assert.assertEquals("mehl", vertex.getProperty("fisch"));
		Assert.assertEquals(5, vertex.getProperty("counter"));
	}

	@Test
	public void test_ChangeSimpleVertex() throws ArangoDBException {
		double numericValue = 6.0;

		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("fisch", "mehl");
		properties.put("counter", 5);
		ArangoDBSimpleVertex vertex = client.createVertex(graph, "egon", properties);
		Assert.assertNotNull(vertex);
		Assert.assertNotNull(vertex.getDocumentKey());
		Assert.assertEquals("mehl", vertex.getProperty("fisch"));
		Assert.assertEquals(5, vertex.getProperty("counter"));

		vertex.removeProperty("fisch");
		vertex.setProperty("counter", numericValue);
		client.saveVertex(graph, vertex);
		Assert.assertNull(vertex.getProperty("fisch"));
		Assert.assertEquals(numericValue, vertex.getProperty("counter"));

		ArangoDBSimpleVertex vertex2 = client.getVertex(graph, vertex.getDocumentKey());
		Assert.assertNotNull(vertex2);
		Assert.assertNotNull(vertex2.getDocumentKey());
		Assert.assertNull(vertex2.getProperty("fisch"));
		Assert.assertEquals(numericValue, vertex2.getProperty("counter"));
	}

	@Test
	public void test_DeleteSimpleVertex() throws ArangoDBException {
		ArangoDBSimpleVertex vertex = client.createVertex(graph, "to_delete", null);
		Assert.assertNotNull(vertex);
		Assert.assertNotNull(vertex.getDocumentKey());

		client.deleteVertex(graph, vertex);
		Assert.assertTrue(vertex.isDeleted());

		try {
			vertex = client.getVertex(graph, "to_delete");
			Assert.fail("ArangoDBException not thrown");
		} catch (ArangoDBException e) {
			Assert.assertEquals(new Integer(ErrorNums.ERROR_ARANGO_DOCUMENT_NOT_FOUND), e.errorNumber());
		}
	}

	@Test
	public void test_SetProperties() throws ArangoDBException {
		ArangoDBSimpleVertex vertex = client.createVertex(graph, "testProperties", null);
		Assert.assertNotNull(vertex);
		Assert.assertNotNull(vertex.getDocumentKey());

		vertex.setProperty("key1", "abc##ääüü");

		HashMap<String, Object> hm = new HashMap<String, Object>();
		hm.put("test", "protest");
		hm.put("hello", "world");
		vertex.setProperty("key2", hm);

		HashMap<Object, Object> hm2 = new HashMap<Object, Object>();
		hm2.put("test", "protest");
		hm2.put(hm, "world"); // fail!
		try {
			vertex.setProperty("key3", hm2);
			Assert.fail("ArangoDBException not thrown");
		} catch (ArangoDBException e) {
			Assert.assertEquals(new Integer(ErrorNums.ERROR_GRAPH_INVALID_PARAMETER), e.errorNumber());
		}

		HashMap<Object, Object> hm3 = new HashMap<Object, Object>();
		hm3.put("test", "protest");
		hm3.put("hello", hm);
		vertex.setProperty("key4", hm3);

		Vector<Object> v = new Vector<Object>();
		v.add("huhu");
		vertex.setProperty("key5", v);

		v.add(hm);
		vertex.setProperty("key6", v);

		v.add(hm2); // fail!
		try {
			vertex.setProperty("key6", v);
			Assert.fail("ArangoDBException not thrown");
		} catch (ArangoDBException e) {
			Assert.assertEquals(new Integer(ErrorNums.ERROR_GRAPH_INVALID_PARAMETER), e.errorNumber());
		}

		client.saveVertex(graph, vertex);
	}

}
