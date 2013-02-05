package com.tinkerpop.blueprints.impls.arangodb.test;

import java.util.Iterator;
import java.util.Vector;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraph;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraphException;

public class ArangoEdgeTest extends ArangoDBTestCase {

	ArangoDBGraph graph = null;
	
	public void testCreateEdge() {
		String id = null;
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			
			Vertex a = graph.addVertex(null);
			Vertex b = graph.addVertex(null);

			Edge e = graph.addEdge(null, a, b, "knows");

			assertEquals("knows", e.getLabel());
			
			// save vertices and edges 
			graph.shutdown();

			assertEquals("knows", e.getLabel());			
			assertTrue(has_id(e));
			id = e.getId().toString();
			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			Edge e = graph.getEdge(id);
			assertEquals("knows", e.getLabel());			
			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}						
	}

	public void testCreateDoubleEdge() {
		boolean thrown = false;
		
		Vertex a = graph.addVertex(null);
		Vertex b = graph.addVertex(null);

		graph.addEdge("123", a, b, "knows");		
		
		try {
			graph.addEdge("123", a, b, "knows");
		}
		catch (IllegalArgumentException e) {
			thrown = true;
		}
		
		assertTrue(thrown);
	}

	public void testCreateEdgeWithLabel() {
		Vertex a = null;
		
		try {			
			a = graph.addVertex(null);
			Vertex b = graph.addVertex(null);

			graph.addEdge("lab_1", a, b, "label1");			
			graph.addEdge("lab_2", a, b, "label2");			
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {			
			// check out and in vertices of vertex b 
			Iterable<Edge> edges = a.getEdges(Direction.BOTH, "label1");
			Iterator<Edge> iterE = edges.iterator();
			assertTrue(iterE.hasNext());
			iterE.next();
			assertFalse(iterE.hasNext());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {			
			// check out and in vertices of vertex b 
			Iterable<Edge> edges = a.getEdges(Direction.BOTH, "label1", "label2");
			Iterator<Edge> iterE = edges.iterator();
			assertTrue(iterE.hasNext());
			iterE.next();
			assertTrue(iterE.hasNext());
			iterE.next();
			assertFalse(iterE.hasNext());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void testVertexCreateManyEdges () {
		int num = 200;
		
		try {			
			Vertex a = graph.addVertex("Vertex_a");
			Vertex b = graph.addVertex("Vertex_b");

			for (int i = 0; i < num; ++i) {
				graph.addEdge("Edge_" + i, a, b, "label " + i);
			}

			Iterable<Edge> edges = graph.getEdges();
			
			assertEquals(num, countElements(edges.iterator()));
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}	

	public void testVertexCreateDeleteManyVertices () {
		int num = 200;
		
		try {		
			Vertex a = graph.addVertex("Vertex_a");
			Vertex b = graph.addVertex("Vertex_b");

			for (int i = 0; i < num; ++i) {
				Edge e = graph.addEdge("Edge_" + i, a, b, "label " + i);
				e.setProperty("intValue", i);
			}

			Iterable<Edge> edges = graph.getEdges();
			assertEquals(num, countElements(edges.iterator()));
			
			edges = graph.getEdges();
			
			Iterator<Edge> iterV = edges.iterator();
			int count = 0;
			while (iterV.hasNext()) {
				++count;
				Edge e = iterV.next();
				graph.removeEdge(e);
				//Vertex v = iterV.next();
				//System.out.println(count + ": " + v.getId());
			}
			assertEquals(num, count);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void testVertexCreateDeletVertices () {	
		try {
			Vertex a = graph.addVertex("Vertex_a");
			Vertex b = graph.addVertex("Vertex_b");
			Edge e = graph.addEdge("to_be_deletd", a, b, "label");
			graph.removeEdge(e);
			graph.removeEdge(e);
			assertTrue(true);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}	

	public void testVertexSetProperty () {
		int expect = 4711;
		try {		
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			Vertex a = graph.addVertex("Vertex_a");
			Vertex b = graph.addVertex("Vertex_b");

			Edge e = graph.addEdge("Edge", a, b, "label");
			e.setProperty("intValue", expect);
			
			graph.shutdown(); // save
			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {		
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);

			Edge e = graph.getEdge("Edge");
			assertEquals(expect, e.getProperty("intValue"));
			
			graph.shutdown(); // save
			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
	}	

	public void testGetGraphEdges () {
		int num = 10;
		
		try {			
			Vector<Vertex> v = new Vector<Vertex>();  
			for (int i = 0; i < 11; ++i) {
				v.add(graph.addVertex(i));
			}
			
			for (int i = 0; i < num; ++i) {
				Edge e = graph.addEdge("Edge_" + i, v.elementAt(i), v.elementAt(i+1), "label " + i);
				e.setProperty("intValue", i);
			}

			Iterable<Edge> edges = graph.getEdges();
			assertEquals(num, countElements(edges.iterator()));

			edges = graph.getEdges("intValue", 5);
			assertEquals(1, countElements(edges.iterator()));

			edges = graph.getEdges("intValue", 100);
			assertEquals(0, countElements(edges.iterator()));
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}	

	public void testVertexGetEdges () {
		int num = 10;
		
		try {			
			Vector<Vertex> v = new Vector<Vertex>();  
			for (int i = 0; i < 11; ++i) {
				v.add(graph.addVertex(i));
			}
			
			for (int i = 0; i < num; ++i) {
				graph.addEdge("Edge_" + i, v.elementAt(i), v.elementAt(i+1), "label " + i);
			}
			for (int i = 0; i < num; ++i) {
				graph.addEdge("Edge_b" + i, v.elementAt(i), v.elementAt(i+1), "label b" + i);
			}

			int index = 5;
			int in = index - 1;
			int out = index;
			Vertex vertex = v.elementAt(index);

			Iterable<Edge> edges = vertex.getEdges(Direction.IN);
			assertEquals(2, countElements(edges.iterator()));
			edges = vertex.getEdges(Direction.IN, "label " + in);
			assertEquals(1, countElements(edges.iterator()));
			edges = vertex.getEdges(Direction.IN, "label " + in, "label " + out);
			assertEquals(1, countElements(edges.iterator()));
 
			edges = vertex.getEdges(Direction.OUT);
			assertEquals(2, countElements(edges.iterator()));
			edges = vertex.getEdges(Direction.OUT, "label " + out);
			assertEquals(1, countElements(edges.iterator()));
			edges = vertex.getEdges(Direction.OUT, "label " + in, "label " + out);
			assertEquals(1, countElements(edges.iterator()));

			edges = vertex.getEdges(Direction.BOTH);
			assertEquals(4, countElements(edges.iterator()));
			edges = vertex.getEdges(Direction.BOTH, "label " + out);
			assertEquals(1, countElements(edges.iterator()));
			edges = vertex.getEdges(Direction.BOTH, "label " + in);
			assertEquals(1, countElements(edges.iterator()));
						
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}	

	public void testGetGraphEdge () {
		Vertex a = null;
		Vertex b = null;
		try {			
			a = graph.addVertex(null);
			b = graph.addVertex(null);

			Edge e1 = graph.addEdge("same", a, b, "label1");
			assertNotNull(e1);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
			
		try {			
			graph.addEdge("same", a, b, "label2");
			fail("inserted edge with same id");
		} catch (IllegalArgumentException e) {
		}
			
	}	
	
	public void testGetGraphEdge2 () {
		Vertex a = null;
		Vertex b = null;
		try {			
			a = graph.addVertex(null);
			b = graph.addVertex(null);

			Edge e1 = graph.addEdge("same", a, b, "label1");
			assertNotNull(e1);
			
			Iterable<Edge> ei = a.getEdges(Direction.OUT);
			assertNotNull(ei);
			
			ei = a.getEdges(Direction.IN);
			assertNotNull(ei);
			
			ei = a.getEdges(Direction.OUT, "label1");
			assertNotNull(ei);			

			ei = a.getEdges(Direction.OUT, "egal");
			assertNotNull(ei);			
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
			
		try {			
			graph.addEdge("same", a, b, "label2");
			fail("inserted edge with same id");
		} catch (IllegalArgumentException e) {
		}
			
	}	
	
	protected void setUp() {
		super.setUp();
		try {
			graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}

	protected void tearDown() {
		graph.shutdown();
		super.tearDown();
	}
	
}
