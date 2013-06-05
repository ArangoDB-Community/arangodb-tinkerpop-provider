package com.tinkerpop.blueprints.impls.arangodb.test;

import java.util.Iterator;
import java.util.Vector;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraph;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraphException;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBVertex;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;

public class ArangoVertexTest extends ArangoDBTestCase {
		
	public void testCreateVertex() {
		Object id = null;
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			
			Vertex a = graph.addVertex(null);
						
			// check for _id
			assertTrue(has_id(a));
			
			id = a.getId();
			
			// shutdown saves the graph
			graph.shutdown();
			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		// reload graph and check for vertex
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			
			Vertex b = graph.getVertex(id);
			assertNotNull(b);

			Vertex a = graph.getVertex(id);			
			boolean eq = b.equals(a); 
			assertTrue(eq);
								
			assertEquals(a, b);
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}						
	}

	public void testGetVertex() {
		Object id = null;
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			Vertex a = graph.addVertex(null);						
			// check for _id
			assertTrue(has_id(a));			
			id = a.getId();
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		// reload graph and check for vertex
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			
			Vertex b = graph.getVertex(id);
			assertNotNull(b);

			Vertex a = graph.getVertex(id);
			
			// check equal
			boolean eq = b.equals(a); 
			assertTrue(eq);								
			assertEquals(a, b);
			
			// get unknown vertex
			Vertex c = graph.getVertex("unknown");
			assertNull(c);
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			assertTrue(false);
			fail(e.getMessage());
		}						
	}
	
	public void testCreateVertexWithAttributes() {
		String vertexId = "v1";
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			
			Vertex a = graph.addVertex(vertexId);
			a.setProperty("key1", "value1");
			a.setProperty("key2", "value2");
						
			// check properties
			assertTrue(hasProperty(a, "key1", "value1"));
			assertTrue(hasProperty(a, "key2", "value2"));
			
			// save vertices and edges 
			graph.shutdown();
			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		// reload graph and check again 
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			
			Vertex a = graph.getVertex(vertexId);

			assertTrue(hasProperty(a, "key1", "value1"));
			assertTrue(hasProperty(a, "key2", "value2"));
			
			graph.shutdown();			
	} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
	}

	public void testCreateVertexIndex() {
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			
			Parameter<String, String> type   = new Parameter<String, String>("type", "skiplist");
			Parameter<String, Boolean> unique = new Parameter<String, Boolean>("unique", false);
			
			graph.createKeyIndex("key1", Vertex.class, type, unique);

			type   = new Parameter<String, String>("type", "hash");
			unique = new Parameter<String, Boolean>("unique", true);
			
			graph.createKeyIndex("key2", Vertex.class, type, unique);
			
			graph.shutdown();			
			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}
	
	public void testCreateVertexWithUniqueIndex() {
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			
			Parameter<String, String> type   = new Parameter<String, String>("type", "skiplist");
			Parameter<String, Boolean> unique = new Parameter<String, Boolean>("unique", true);
			
			graph.createKeyIndex("key1", Vertex.class, type, unique);
			
			ArangoDBVertex a = (ArangoDBVertex) graph.addVertex("v1");
			a.setProperty("key1", "value1");
			a.save();
						
			ArangoDBVertex b = (ArangoDBVertex) graph.addVertex("v2");
			b.setProperty("key1", "value1");
			b.save(); // this fails
			
			assertTrue(false);
			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (ArangoDBException e) {
			assertTrue(true);
		}
	}
	
	public void testCreateVertexWithUniqueIndexAndNoDelay() {
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			graph.setDelaySaveUpdates(false);
			
			Parameter<String, String> type   = new Parameter<String, String>("type", "skiplist");
			Parameter<String, Boolean> unique = new Parameter<String, Boolean>("unique", true);
			
			graph.createKeyIndex("key1", Vertex.class, type, unique);
			
			Vertex a = graph.addVertex("v1");
			a.setProperty("key1", "value1");
						
			Vertex b = graph.addVertex("v2");
			b.setProperty("key1", "value1"); // this fails
			
			assertTrue(false);
			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
	}
	
	public void testCreateDoubleVertex() {
		Integer id = 123;
		boolean thrown = false;
		ArangoDBGraph graph = null;
		
		try {			
			graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			graph.addVertex(id);

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}	
		
		try {
			graph.addVertex(id);

		} catch (Exception e) {
			// double id
			thrown = true;
		}
		
		assertTrue(thrown);
	}
	
	public void testCreateGraphWithEdge() {
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			
			Vertex a = graph.addVertex(null);
			a.setProperty("name", "marko");

			Vertex b = graph.addVertex(null);
			b.setProperty("name", "peter");

			Edge e = graph.addEdge(null, a, b, "knows");
			
			// System.out.println(e.getVertex(Direction.OUT).getProperty("name") + "--" + e.getLabel() + "-->" + e.getVertex(Direction.IN).getProperty("name"));
			
			// save vertices and edges 
			graph.shutdown();
			
			assertTrue(has_id(e));
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}				
	}

	public void testVertexGetEdges1 () {
		String e1Id = null; 
		String e2Id = null; 
		String bId = null;
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			Vertex a = graph.addVertex(null);
			Vertex b = graph.addVertex(null);
			Vertex c = graph.addVertex(null);

			Edge e1 = graph.addEdge(null, a, b, "knows");
			Edge e2 = graph.addEdge(null, b, c, "knows");
			graph.shutdown();
			
			assertTrue(has_id(a));
			assertTrue(has_id(b));
			assertTrue(has_id(c));
			assertTrue(has_id(e1));
			assertTrue(has_id(e2));

			// check edge e1 (a -> b) 
			Iterable<Edge> edges = b.getEdges(Direction.IN);
			Iterator<Edge> iterE = edges.iterator();
			assertTrue(iterE.hasNext());
			Edge e = iterE.next();
			assertEquals(e.getId(), e1.getId());
			assertFalse(iterE.hasNext());

			// check edge e2 (b -> c) 
			edges = b.getEdges(Direction.OUT);
			iterE = edges.iterator();
			assertTrue(iterE.hasNext());
			e = iterE.next();
			assertEquals(e.getId(), e2.getId());
			assertFalse(iterE.hasNext());

			e1Id = e1.getId().toString(); 
			e2Id = e2.getId().toString();			
			bId = b.getId().toString();
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
		
		// reload data and test again
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			Vertex b = graph.getVertex(bId);
			Edge e1 = graph.getEdge(e1Id);
			Edge e2 = graph.getEdge(e2Id);

			// check edge e1 (a -> b) 
			Iterable<Edge> edges = b.getEdges(Direction.IN);
			Iterator<Edge> iterE = edges.iterator();
			assertTrue(iterE.hasNext());
			Edge e = iterE.next();
			assertEquals(e.getId(), e1.getId());
			assertFalse(iterE.hasNext());

			// check edge e2 (b -> c) 
			edges = b.getEdges(Direction.OUT);
			iterE = edges.iterator();
			assertTrue(iterE.hasNext());
			e = iterE.next();
			assertEquals(e.getId(), e2.getId());
			assertFalse(iterE.hasNext());
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}		
	}

	public void testVertexGetEdges2 () {
		String bId = null;
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			Vertex a = graph.addVertex(null);
			Vertex b = graph.addVertex(null);
			Vertex c = graph.addVertex(null);

			graph.addEdge(null, a, b, "knows");
			graph.addEdge(null, b, c, "knows");
			graph.shutdown(); // save
			
			// check edges of vertex b 
			Iterable<Edge> edges = b.getEdges(Direction.BOTH);
			Iterator<Edge> iterE = edges.iterator();
			assertTrue(iterE.hasNext());
			iterE.next();
			assertTrue(iterE.hasNext());
			iterE.next();
			assertFalse(iterE.hasNext());

			bId = b.getId().toString();
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
		
		// reload data and test again
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			Vertex b = graph.getVertex(bId);
			
			// check edges of vertex b 
			Iterable<Edge> edges = b.getEdges(Direction.BOTH);
			Iterator<Edge> iterE = edges.iterator();
			assertTrue(iterE.hasNext());
			iterE.next();
			assertTrue(iterE.hasNext());
			iterE.next();
			assertFalse(iterE.hasNext());
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}

	public void testVertexGetVertices1 () {
		String aId = null; 
		String bId = null; 
		String cId = null;
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			Vertex a = graph.addVertex(null);
			Vertex b = graph.addVertex(null);
			Vertex c = graph.addVertex(null);

			graph.addEdge(null, a, b, "knows");
			graph.addEdge(null, b, c, "knows");
			graph.shutdown();
			
			// check in vertex of edge e1 (a -> b) 
			Iterable<Vertex> vertices = b.getVertices(Direction.IN);
			Iterator<Vertex> iterV = vertices.iterator();
			assertTrue(iterV.hasNext());
			Vertex v = iterV.next();
			assertEquals(v.getId(), a.getId());
			assertFalse(iterV.hasNext());
			
			// check out vertex of edge e2 (b -> c) 
			vertices = b.getVertices(Direction.OUT);
			iterV = vertices.iterator();
			assertTrue(iterV.hasNext());
			v = iterV.next();
			assertEquals(v.getId(), c.getId());
			assertFalse(iterV.hasNext());

			aId = a.getId().toString(); 
			bId = b.getId().toString();
			cId = c.getId().toString();
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
		
		// reload data and test again
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			Vertex a = graph.getVertex(aId);
			Vertex b = graph.getVertex(bId);
			Vertex c = graph.getVertex(cId);

			// check in vertex of edge e1 (a -> b) 
			Iterable<Vertex> vertices = b.getVertices(Direction.IN);
			Iterator<Vertex> iterV = vertices.iterator();
			assertTrue(iterV.hasNext());
			Vertex v = iterV.next();
			assertEquals(v.getId(), a.getId());
			assertFalse(iterV.hasNext());
			
			// check out vertex of edge e2 (b -> c) 
			vertices = b.getVertices(Direction.OUT);
			iterV = vertices.iterator();
			assertTrue(iterV.hasNext());
			v = iterV.next();
			assertEquals(v.getId(), c.getId());
			assertFalse(iterV.hasNext());
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}		
	}

	public void testVertexGetVertices2 () {
		String bId = null; 
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			Vertex a = graph.addVertex(null);
			Vertex b = graph.addVertex(null);
			Vertex c = graph.addVertex(null);

			graph.addEdge(null, a, b, "knows");
			graph.addEdge(null, b, c, "knows");
			
			// check out and in vertices of vertex b 
			Iterable<Vertex> vertices = b.getVertices(Direction.BOTH);
			Iterator<Vertex> iterV = vertices.iterator();
			assertTrue(iterV.hasNext());
			Vertex v = iterV.next();
			assertNotSame(v.getId(), b.getId());
			assertTrue(iterV.hasNext());
			v = iterV.next();
			assertNotSame(v.getId(), b.getId());
			assertFalse(iterV.hasNext());
			
			bId = b.getId().toString();			
			graph.shutdown(); // save
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
		
		// reload data and test again
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			Vertex b = graph.getVertex(bId);

			// check out and in vertices of vertex b 
			Iterable<Vertex> vertices = b.getVertices(Direction.BOTH);
			Iterator<Vertex> iterV = vertices.iterator();
			assertTrue(iterV.hasNext());
			Vertex v = iterV.next();
			assertNotSame(v.getId(), b.getId());
			assertTrue(iterV.hasNext());
			v = iterV.next();
			assertNotSame(v.getId(), b.getId());
			assertFalse(iterV.hasNext());
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}	

	public void testVertexDeleteVertex () {
		String bId = null; 
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			Vertex a = graph.addVertex(null);
			Vertex b = graph.addVertex(null);
			Vertex c = graph.addVertex(null);

			graph.addEdge(null, a, b, "knows");
			graph.addEdge(null, b, c, "knows");
			
			graph.removeVertex(a);
			
			// check out and in vertices of vertex b 
			Iterable<Vertex> vertices = b.getVertices(Direction.BOTH);
			Iterator<Vertex> iterV = vertices.iterator();
			assertTrue(iterV.hasNext());
			Vertex v = iterV.next();
			assertNotSame(v.getId(), b.getId());
			assertFalse(iterV.hasNext());
			
			bId = b.getId().toString();
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
		
		// reload data and test again
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);			
			Vertex b = graph.getVertex(bId);

			// check out and in vertices of vertex b 
			Iterable<Vertex> vertices = b.getVertices(Direction.BOTH);
			Iterator<Vertex> iterV = vertices.iterator();
			assertTrue(iterV.hasNext());
			Vertex v = iterV.next();
			assertNotSame(v.getId(), b.getId());
			assertFalse(iterV.hasNext());
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}	

	
	public void testVertexCreateManyVertices () {
		int num = 200;
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			for (int i = 0; i < num; ++i) {
				graph.addVertex("Vertex" + i);
			}

			Iterable<Vertex> vertices = graph.getVertices();
			
			Iterator<Vertex> iterV = vertices.iterator();
			int count = 0;
			while (iterV.hasNext()) {
				++count;
				Vertex v = iterV.next();
				assertNotNull(v);
				
				//System.out.println(count + ": " + v.getId());
			}
			assertEquals(num, count);

			Iterator<Vertex> iterV2 = vertices.iterator();
			int count2 = 0;
			while (iterV2.hasNext()) {
				++count2;
				iterV2.next();
				//Vertex v = iterV.next();
				//System.out.println(count + ": " + v.getId());
			}
			assertEquals(num, count2);

			count = 0;
            for (final Vertex vertex : graph.getVertices()) {
            	++count;
            	assertNotNull(vertex);
            }			
			assertEquals(num, count);
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}	

	public void testVertexCreateDelteManyVertices () {
		int num = 200;
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			for (int i = 0; i < num; ++i) {
				graph.addVertex("Vertex" + i);
			}

			Iterable<Vertex> vertices = graph.getVertices();
			
			Iterator<Vertex> iterV = vertices.iterator();
			int count = 0;
			while (iterV.hasNext()) {
				++count;
				Vertex v = iterV.next();
				//System.out.println(count + ": " + v.getId());
				graph.removeVertex(v);
			}
			assertEquals(num, count);
			
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}	
	
	public void testVertexGetVertices () {
		int num = 10;
		
		try {			
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);

			Vector<Vertex> v = new Vector<Vertex>();  
			for (int i = 0; i < 11; ++i) {
				v.add(graph.addVertex(i));
			}
			
			for (int i = 0; i < num; ++i) {
				graph.addEdge("Edge_a" + i, v.elementAt(i), v.elementAt(i+1), "label a" + i);
			}
			
			for (int i = 0; i < num; ++i) {
				graph.addEdge("Edge_b" + i, v.elementAt(i), v.elementAt(i+1), "label b" + i);
			}

			int index = 5;
			int in = index - 1;
			int out = index;
			Vertex vertex = v.elementAt(index);

			Iterable<Vertex> vertices = vertex.getVertices(Direction.IN);
			assertEquals(2, countElements(vertices.iterator()));
			vertices = vertex.getVertices(Direction.IN, "label a" + in);
			assertEquals(1, countElements(vertices.iterator()));
			vertices = vertex.getVertices(Direction.IN, "label a" + in, "label a" + out);
			assertEquals(1, countElements(vertices.iterator()));
 
			vertices = vertex.getVertices(Direction.OUT);
			assertEquals(2, countElements(vertices.iterator()));
			vertices = vertex.getVertices(Direction.OUT, "label a" + out);
			assertEquals(1, countElements(vertices.iterator()));
			vertices = vertex.getVertices(Direction.OUT, "label a" + in, "label a" + out);
			assertEquals(1, countElements(vertices.iterator()));

			vertices = vertex.getVertices(Direction.BOTH);
			assertEquals(4, countElements(vertices.iterator()));
			vertices = vertex.getVertices(Direction.BOTH, "label a" + out);
			assertEquals(1, countElements(vertices.iterator()));
			vertices = vertex.getVertices(Direction.BOTH, "label a" + in);
			assertEquals(1, countElements(vertices.iterator()));
						
			graph.shutdown();			
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}	
	
	
}
