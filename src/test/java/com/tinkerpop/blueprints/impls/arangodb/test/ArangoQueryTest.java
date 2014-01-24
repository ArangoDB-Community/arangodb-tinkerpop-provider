package com.tinkerpop.blueprints.impls.arangodb.test;

import java.util.Iterator;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraph;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraphException;

public class ArangoQueryTest extends ArangoDBTestCase {

	public void testVertexQuery() {
		int num = 200;

		try {
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			Vertex a = graph.addVertex("Vertex_a");
			Vertex b = graph.addVertex("Vertex_b");

			for (int i = 0; i < num; ++i) {
				graph.addEdge("Edge_" + i, a, b, "label " + i);
			}

			VertexQuery q = a.query();

			Iterable<Edge> edges = q.edges();

			Iterator<Edge> iterV = edges.iterator();
			int count = 0;
			while (iterV.hasNext()) {
				++count;
				iterV.next();
				// graph.removeEdge(e);
				// System.out.println(count + ": " + v.getId());
			}
			assertEquals(num, count);

			graph.shutdown(); // save

		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}
	}

	public void testVertexQueryByLabel() {
		int num = 200;

		try {
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			Vertex a = graph.addVertex("Vertex_a");
			Vertex b = graph.addVertex("Vertex_b");

			for (int i = 0; i < num; ++i) {
				graph.addEdge("Edge_" + i, a, b, "label " + i);
			}

			VertexQuery q = a.query();
			q.labels("label 10", "label 100", "label 199");

			Iterable<Edge> edges = q.edges();

			Iterator<Edge> iterV = edges.iterator();
			int count = 0;
			while (iterV.hasNext()) {
				++count;
				iterV.next();
				// graph.removeEdge(e);
				// System.out.println(count + ": " + v.getId());
			}
			assertEquals(3, count);

			graph.shutdown(); // save

		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}
	}

	public void testVertexQueryByKey() {
		int num = 50;

		try {
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			Vertex a = graph.addVertex("Vertex_a");
			Vertex b = graph.addVertex("Vertex_b");

			for (int i = 0; i < num; ++i) {
				Edge e = graph.addEdge("Edge_" + i, a, b, "label " + i);
				e.setProperty("intValue", i);
			}

			VertexQuery q = a.query();
			q.has("intValue", 33);

			Iterable<Edge> edges = q.edges();

			Iterator<Edge> iterV = edges.iterator();
			int count = 0;
			while (iterV.hasNext()) {
				++count;
				iterV.next();
			}
			assertEquals(1, count);

			q.direction(Direction.OUT);
			edges = q.edges();

			iterV = edges.iterator();
			count = 0;
			while (iterV.hasNext()) {
				++count;
				iterV.next();
			}
			assertEquals(1, count);
			assertEquals(1, q.count());

			q.direction(Direction.IN);
			edges = q.edges();

			iterV = edges.iterator();
			assertFalse(iterV.hasNext());
			assertEquals(0, q.count());

			graph.shutdown(); // save

		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}
	}

	/*
	 * public void testVertexQueryByInterval () { int num = 30;
	 * 
	 * try { ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName,
	 * vertices, edges); Vertex a = graph.addVertex("Vertex_a"); Vertex b =
	 * graph.addVertex("Vertex_b");
	 * 
	 * for (int i = 0; i < num; ++i) { Edge e = graph.addEdge("Edge_" + i, a, b,
	 * "label " + i); e.setProperty("intValue", i); }
	 * 
	 * VertexQuery q = a.query(); q.interval("intValue", 10, 12);
	 * 
	 * Iterable<Edge> edges = q.edges();
	 * 
	 * Iterator<Edge> iterV = edges.iterator(); int count = 0; while
	 * (iterV.hasNext()) { ++count; iterV.next(); } assertEquals(2, count);
	 * 
	 * graph.shutdown(); // save
	 * 
	 * } catch (ArangoDBGraphException e) { e.printStackTrace();
	 * fail("Could not create graph"); } }
	 */
	public void testVertexQueryIDs() {
		int num = 7;

		try {
			ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			Vertex a = graph.addVertex("Vertex_a");
			Vertex b = graph.addVertex("Vertex_b");

			for (int i = 0; i < num; ++i) {
				Edge e = graph.addEdge("Edge_" + i, a, b, "label " + i);
				e.setProperty("intValue", i);
			}

			VertexQuery q = a.query();
			q.has("intValue", 3);
			q.direction(Direction.OUT);

			Iterator<String> iter = (Iterator<String>) q.vertexIds();
			int count = 0;
			while (iter.hasNext()) {
				++count;
				iter.next();
			}
			assertEquals(1, count);

			graph.shutdown(); // save

		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			fail("Could not create graph");
		}
	}

	// public void testVertexQueryHas() {
	// int num = 7;
	//
	// try {
	// ArangoDBGraph graph = new ArangoDBGraph(configuration, graphName,
	// vertices, edges);
	// Vertex a = graph.addVertex("Vertex_a");
	// Vertex b = graph.addVertex("Vertex_b");
	//
	// for (int i = 0; i < num; ++i) {
	// Edge e = graph.addEdge("Edge_" + i, a, b, "label " + i);
	// e.setProperty("intValue" + i, i);
	// }
	//
	// VertexQuery q = a.query();
	// q.has("intValue1");
	//
	// Iterator<String> iter = (Iterator<String>) q.vertexIds();
	// int count = 0;
	// while (iter.hasNext()) {
	// ++count;
	// iter.next();
	// }
	// assertEquals(1, count);
	//
	// graph.shutdown(); // save
	//
	// } catch (ArangoDBGraphException e) {
	// e.printStackTrace();
	// fail("Could not create graph");
	// }
	// }
}
