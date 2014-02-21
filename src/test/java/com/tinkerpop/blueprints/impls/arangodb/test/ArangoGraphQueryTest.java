package com.tinkerpop.blueprints.impls.arangodb.test;

import java.util.Iterator;

import org.junit.Test;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraph;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraphException;

public class ArangoGraphQueryTest extends ArangoDBTestCase {

	private ArangoDBGraph graph;
	private Vertex a;
	private Vertex b;
	private Vertex c;

	protected void setUp() {
		super.setUp();

		int num = 7;

		try {
			graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
			a = graph.addVertex("Vertex_a");
			b = graph.addVertex("Vertex_b");
			c = graph.addVertex("Vertex_c");

			a.setProperty("a", "b");
			b.setProperty("a", "c");
			b.setProperty("i", "j");
			c.setProperty("x", "y");

			for (int i = 0; i < num; ++i) {
				Edge e = graph.addEdge("Edge_" + i, a, b, "label " + i);
				e.setProperty("intValue" + i, i);
			}
		} catch (ArangoDBGraphException e1) {
			e1.printStackTrace();
		}
	}

	protected void tearDown() {
		graph.shutdown();

		super.tearDown();
	}

	@Test
	public void testGraphQuery() {
		GraphQuery q = graph.query();

		assertEquals(3, countResults(q.vertices()));
	}

	@Test
	public void testGraphQueryHas() {
		GraphQuery q = graph.query();
		q.has("x");

		assertEquals(1, countResults(q.vertices()));
	}

	@Test
	public void testGraphQueryHas2() {
		GraphQuery q = graph.query();
		q.has("x");
		q.has("a");

		assertEquals(0, countResults(q.vertices()));
	}

	@Test
	public void testGraphQueryHas3() {
		GraphQuery q = graph.query();
		q.has("i");
		q.has("a");

		assertEquals(1, countResults(q.vertices()));
	}

	@Test
	public void testGraphQueryHasNot() {
		GraphQuery q = graph.query();
		q.hasNot("x");

		assertEquals(2, countResults(q.vertices()));
	}

	@Test
	public void testGraphQueryHasNot2() {
		GraphQuery q = graph.query();
		q.hasNot("a");

		assertEquals(1, countResults(q.vertices()));
	}

	private int countResults(Iterable<?> iterable) {
		Iterator<?> iter = iterable.iterator();

		int count = 0;
		while (iter.hasNext()) {
			++count;
			iter.next();
		}

		return count;
	}

}
