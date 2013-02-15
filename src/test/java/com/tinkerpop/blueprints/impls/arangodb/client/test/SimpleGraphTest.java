package com.tinkerpop.blueprints.impls.arangodb.client.test;

import com.tinkerpop.blueprints.impls.arangodb.client.*;

public class SimpleGraphTest extends BaseTestCase {
	
	protected void setUp() {
		super.setUp();
	}

	protected void tearDown() {
		super.tearDown();
	}

	public void test_CreateSimpleGraph () {
		
		ArangoDBSimpleGraph graph = null;
		try {
			
			graph = client.createGraph(graphName, vertices, edges);
			
			assertNotNull(graph);		
			assertEquals(graphName, graph.getName());
			assertEquals(vertices, graph.getVertexCollection());
			assertEquals(edges, graph.getEdgeCollection());
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
		
		assertNotNull(graph);		
	}

	
	public void test_GetSimpleGraph () {
		
		ArangoDBSimpleGraph graph = null;
		ArangoDBSimpleGraph graph2 = null;
		try {
			
			graph = client.createGraph(graphName, vertices, edges);
			
			assertNotNull(graph);		
			assertEquals(graphName, graph.getName());
			assertEquals(vertices, graph.getVertexCollection());
			assertEquals(edges, graph.getEdgeCollection());
			
			graph2 = client.getGraph(graphName);
			assertNotNull(graph2);		
			assertEquals(graphName, graph2.getName());
			assertEquals(vertices, graph2.getVertexCollection());
			assertEquals(edges, graph2.getEdgeCollection());
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
		
		assertNotNull(graph);		
		assertNotNull(graph2);		
	}

	
	public void test_DeleteSimpleGraph () {
		
		ArangoDBSimpleGraph graph = null;
		ArangoDBSimpleGraph graph2 = null;

		try {
			
			graph = client.createGraph(graphName, vertices, edges);
			
			assertNotNull(graph);		
			assertEquals(graphName, graph.getName());
			assertEquals(vertices, graph.getVertexCollection());
			assertEquals(edges, graph.getEdgeCollection());
			
			graph2 = client.getGraph(graphName);
			assertNotNull(graph2);		
			assertEquals(graphName, graph2.getName());
			assertEquals(vertices, graph2.getVertexCollection());
			assertEquals(edges, graph2.getEdgeCollection());

			boolean b = client.deleteGraph(graph2);
			assertTrue(b);

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
			
		try {
			graph2 = client.getGraph(graphName);
			assertNull(graph2);		
		} catch (ArangoDBException e) {
			assertTrue(true);
		}
	}

	public void test_ReCreateGraph () {
		
		ArangoDBSimpleGraph graph = null;
		ArangoDBSimpleGraph graph2 = null;

		try {
			
			graph = client.createGraph(graphName, vertices, edges);
			
			assertNotNull(graph);		
			assertEquals(graphName, graph.getName());
			assertEquals(vertices, graph.getVertexCollection());
			assertEquals(edges, graph.getEdgeCollection());
			
			graph2 = client.createGraph(graphName, vertices, edges);
			assertTrue(false);		

		} catch (ArangoDBException e) {
			assertTrue(true);		
		}
			
	}
	
}
