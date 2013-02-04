package com.tinkerpop.blueprints.impls.arangodb.client.test;

import com.tinkerpop.blueprints.impls.arangodb.client.*;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class SimpleEdgeTest extends BaseTestCase {
	
	ArangoDBSimpleGraph graph = null;
	ArangoDBSimpleVertex vertex1 = null;
	ArangoDBSimpleVertex vertex2 = null;
	ArangoDBSimpleVertex vertex3 = null;
	ArangoDBSimpleVertex vertex4 = null;
	ArangoDBSimpleVertex vertex5 = null;
	
	protected void setUp() {
		super.setUp();
		try {			
			graph = client.createGraph(graphName, vertices, edges);
			vertex1 = client.createVertex(graph, "v1", null);
			vertex2 = client.createVertex(graph, "v2", null);
			vertex3 = client.createVertex(graph, "v3", null);
			vertex4 = client.createVertex(graph, "v4", null);
			vertex5 = client.createVertex(graph, "v5", null);
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
	}

	protected void tearDown() {
		super.tearDown();
	}

	public void test_CreateSimpleEdge () {
		
		ArangoDBSimpleEdge edge = null;
		try {
			
			edge = client.createEdge(graph, null, null, vertex1, vertex2, null);
			
			assertNotNull(edge);		
			assertNotNull(edge.getDocumentKey());		
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
		
		assertNotNull(edge);		
	}

	public void test_CreateSimpleEdgeWithLabel () {
		
		ArangoDBSimpleEdge edge = null;
		try {			
			edge = client.createEdge(graph, "edge1", "label1", vertex1, vertex2, null);
			
			assertNotNull(edge);		
			assertNotNull(edge.getDocumentKey());
			assertEquals("edge1", edge.getName());
			assertEquals("label1", edge.getLabel());
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
		
		assertNotNull(edge);		
	}

	public void test_CreateSimpleEdgeWithProperties () {
		
		ArangoDBSimpleEdge edge = null;
		try {			
			JSONObject properties = new JSONObject();
			properties.put("hello", "world");
			properties.put("key", "value");			
			
			edge = client.createEdge(graph, "edge1", "label1", vertex1, vertex2, properties);
			
			assertNotNull(edge);		
			assertNotNull(edge.getDocumentKey());
			assertEquals("edge1", edge.getName());
			assertEquals("label1", edge.getLabel());
			assertEquals("world", edge.getProperty("hello"));
			assertEquals("value", edge.getProperty("key"));
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
		
		assertNotNull(edge);		
	}

	public void test_GetSimpleEdge () {
		
		ArangoDBSimpleEdge edge = null;
		ArangoDBSimpleEdge edge2 = null;
		try {			
			JSONObject properties = new JSONObject();
			properties.put("hello", "world");
			properties.put("key", "value");			
			
			edge = client.createEdge(graph, "edge1", "label1", vertex1, vertex2, properties);

			edge2 = client.getEdge(graph, "edge1");

			
			assertNotNull(edge2);		
			assertNotNull(edge2.getDocumentKey());
			assertEquals("edge1", edge2.getName());
			assertEquals("label1", edge2.getLabel());
			assertEquals("world", edge2.getProperty("hello"));
			assertEquals("value", edge2.getProperty("key"));
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
		
		assertNotNull(edge);		
	}

	public void test_ChangeSimpleEdge () {
		
		ArangoDBSimpleEdge edge = null;
		ArangoDBSimpleEdge edge2 = null;
		try {			
			JSONObject properties = new JSONObject();
			properties.put("name", "mayer");			
			properties.put("street", "barmer");			
			
			edge = client.createEdge(graph, "edge1", "label1", vertex1, vertex2, properties);

			edge2 = client.getEdge(graph, "edge1");
			
			assertNotNull(edge2);		
			assertNotNull(edge2.getDocumentKey());
			assertEquals("mayer", edge2.getProperty("name"));
			assertEquals("barmer", edge2.getProperty("street"));
			
			edge2.removeProperty("street");
			edge2.setProperty("name", "mueller");
			
			client.saveEdge(graph, edge2);
			assertEquals("mueller", edge2.getProperty("name"));
			assertNull(edge2.getProperty("street"));
			
			ArangoDBSimpleEdge edge3 = client.getEdge(graph, "edge1");
			
			assertNotNull(edge3);		
			assertNotNull(edge3.getDocumentKey());
			assertEquals("mueller", edge3.getProperty("name"));
			assertNull(edge3.getProperty("street"));
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
		
		assertNotNull(edge);		
	}

	public void test_DeleteSimpleEdge () {
		
		ArangoDBSimpleEdge edge = null;
		String key = "";
		try {
			
			edge = client.createEdge(graph, null, null, vertex1, vertex2, null);
			
			assertNotNull(edge);		
			assertNotNull(edge.getDocumentKey());
			
			key = edge.getDocumentKey();
			
			boolean b = client.deleteEdge(graph, edge);
			assertTrue(b);
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
				
		try {
			ArangoDBSimpleEdge edge2 = client.getEdge(graph, key);						
			assertNull(edge2);		
			assertTrue(false);								
		} catch (ArangoDBException e) {
			assertTrue(true);		
		}
	}
	
}
