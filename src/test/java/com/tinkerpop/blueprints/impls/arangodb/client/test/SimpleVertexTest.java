package com.tinkerpop.blueprints.impls.arangodb.client.test;

import java.util.HashMap;
import java.util.Vector;

import com.tinkerpop.blueprints.impls.arangodb.client.*;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class SimpleVertexTest extends BaseTestCase {

	ArangoDBSimpleGraph graph = null;

	protected void setUp() {
		super.setUp();
		try {
			graph = client.createGraph(graphName, vertices, edges);

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

	protected void tearDown() {
		super.tearDown();
	}

	public void test_CreateSimpleVertex() {

		ArangoDBSimpleVertex vertex = null;
		try {

			vertex = client.createVertex(graph, null, null);

			assertNotNull(vertex);
			assertNotNull(vertex.getDocumentKey());

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		}

		assertNotNull(vertex);
	}

	public void test_CreateAndGetSimpleVertex() {

		ArangoDBSimpleVertex vertex = null;
		ArangoDBSimpleVertex vertex2 = null;
		ArangoDBSimpleVertex vertex3 = null;
		try {

			vertex = client.createVertex(graph, null, null);

			assertNotNull(vertex);
			assertNotNull(vertex.getDocumentKey());

			vertex2 = client.getVertex(graph, vertex.getDocumentKey());

			assertNotNull(vertex2);
			assertNotNull(vertex2.getDocumentKey());

			vertex3 = client.getVertex(graph, vertex.getDocumentId());

			assertNotNull(vertex3);
			assertNotNull(vertex3.getDocumentKey());

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		}

		assertNotNull(vertex);
	}

	public void test_CreateSimpleVertexWithName() {

		ArangoDBSimpleVertex vertex = null;
		try {

			vertex = client.createVertex(graph, "egon", null);

			assertNotNull(vertex);
			assertNotNull(vertex.getDocumentKey());
			assertEquals("egon", vertex.getDocumentKey());

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		}

		assertNotNull(vertex);
	}

	public void test_CreateSimpleVertexWithProperties() {

		ArangoDBSimpleVertex vertex = null;
		try {

			JSONObject properties = new JSONObject();
			properties.put("fisch", "mehl");
			properties.put("counter", 5);

			vertex = client.createVertex(graph, "egon", properties);

			assertNotNull(vertex);
			assertNotNull(vertex.getDocumentKey());
			assertEquals("mehl", vertex.getProperty("fisch"));
			assertEquals(5, vertex.getProperty("counter"));

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);
		}

		assertNotNull(vertex);
	}

	public void test_ChangeSimpleVertex() {

		ArangoDBSimpleVertex vertex = null;
		ArangoDBSimpleVertex vertex2 = null;
		try {

			JSONObject properties = new JSONObject();
			properties.put("fisch", "mehl");
			properties.put("counter", 5);
			vertex = client.createVertex(graph, "egon", properties);
			assertNotNull(vertex);
			assertNotNull(vertex.getDocumentKey());
			assertEquals("mehl", vertex.getProperty("fisch"));
			assertEquals(5, vertex.getProperty("counter"));

			vertex.removeProperty("fisch");
			vertex.setProperty("counter", 6);
			client.saveVertex(graph, vertex);
			assertNull(vertex.getProperty("fisch"));
			assertEquals(6, vertex.getProperty("counter"));

			vertex2 = client.getVertex(graph, vertex.getDocumentKey());
			assertNotNull(vertex2);
			assertNotNull(vertex2.getDocumentKey());
			assertNull(vertex2.getProperty("fisch"));
			assertEquals(6, vertex2.getProperty("counter"));

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);
		}

		assertNotNull(vertex);
	}

	public void test_DeleteSimpleVertex() {

		ArangoDBSimpleVertex vertex = null;
		try {

			vertex = client.createVertex(graph, "to_delete", null);
			assertNotNull(vertex);
			assertNotNull(vertex.getDocumentKey());

			client.deleteVertex(graph, vertex);
			assertTrue(vertex.isDeleted());

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		}

		try {
			vertex = client.getVertex(graph, "to_delete");
			assertTrue(false);
		} catch (ArangoDBException e) {
			assertTrue(true);
		}

		assertNotNull(vertex);
	}

	public void test_SetProperties() {

		try {

			ArangoDBSimpleVertex vertex = client.createVertex(graph,
					"testProperties", null);
			assertNotNull(vertex);
			assertNotNull(vertex.getDocumentKey());

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
				assertTrue(false);
			} catch (ArangoDBException e) {
			}

			HashMap<Object, Object> hm3 = new HashMap<Object, Object>();
			hm3.put("test", "protest");
			hm3.put("hello", hm);
			try {
				vertex.setProperty("key4", hm3);
			} catch (ArangoDBException e) {
				assertTrue(false);
			}

			Vector<Object> v = new Vector<Object>();
			v.add("huhu");
			try {
				vertex.setProperty("key5", v);
			} catch (ArangoDBException e) {
				assertTrue(false);
			}

			v.add(hm);
			try {
				vertex.setProperty("key6", v);
			} catch (ArangoDBException e) {
				assertTrue(false);
			}

			v.add(hm2); // fail!
			try {
				vertex.setProperty("key6", v);
				assertTrue(false);
			} catch (ArangoDBException e) {
			}

			client.saveVertex(graph, vertex);

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		}

	}

}
