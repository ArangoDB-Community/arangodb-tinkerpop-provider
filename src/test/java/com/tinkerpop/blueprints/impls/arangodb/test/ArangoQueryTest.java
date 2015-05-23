package com.tinkerpop.blueprints.impls.arangodb.test;

import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraph;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraphException;

public class ArangoQueryTest extends ArangoDBTestCase {

	private ArangoDBGraph graph;

	@Before
	protected void setUp() {
		super.setUp();
		try {
			graph = new ArangoDBGraph(configuration, graphName, vertices, edges);
		} catch (ArangoDBGraphException e1) {
			e1.printStackTrace();
		}
	}

	@After
	protected void tearDown() {
		graph.shutdown();

		super.tearDown();
	}

	@Test
	public void testVertexQuery() {
		int num = 200;

		Vertex a = graph.addVertex("Vertex_a");
		Vertex b = graph.addVertex("Vertex_b");

		for (int i = 0; i < num; ++i) {
			graph.addEdge("Edge_" + i, a, b, "label " + i);
		}

		VertexQuery q = a.query();
		assertEquals(num, countResults(q.edges()));

	}

	@Test
	public void testVertexQueryByLabel() {
		int num = 200;

		Vertex a = graph.addVertex("Vertex_a");
		Vertex b = graph.addVertex("Vertex_b");

		for (int i = 0; i < num; ++i) {
			graph.addEdge("Edge_" + i, a, b, "label " + i);
		}

		VertexQuery q = a.query();
		q.labels("label 10", "label 100", "label 199");

		assertEquals(3, countResults(q.edges()));

	}

	@Test
	public void testVertexQueryByKey() {
		int num = 50;

		Vertex a = graph.addVertex("Vertex_a");
		Vertex b = graph.addVertex("Vertex_b");

		for (int i = 0; i < num; ++i) {
			Edge e = graph.addEdge("Edge_" + i, a, b, "label " + i);
			e.setProperty("intValue", i);
		}

		VertexQuery q = a.query();
		q.has("intValue", 33);

		assertEquals(1, countResults(q.edges()));

		q.direction(Direction.OUT);

		assertEquals(1, countResults(q.edges()));
		assertEquals(1, q.count());

		q.direction(Direction.IN);
		assertEquals(0, countResults(q.edges()));
		assertEquals(0, q.count());

	}

	@Test
	public void testVertexQueryByInterval() {
		int num = 30;

		Vertex a = graph.addVertex("Vertex_a");
		Vertex b = graph.addVertex("Vertex_b");

		for (int i = 0; i < num; ++i) {
			Edge e = graph.addEdge("Edge_" + i, a, b, "label " + i);
			e.setProperty("intValue", i);
		}

		VertexQuery q = a.query();
		q.interval("intValue", 10, 12);

		assertEquals(2, countResults(q.edges()));
	}

	@Test
	public void testVertexQueryIDs() {
		int num = 7;

		Vertex a = graph.addVertex("Vertex_a");
		Vertex b = graph.addVertex("Vertex_b");

		for (int i = 0; i < num; ++i) {
			Edge e = graph.addEdge("Edge_" + i, a, b, "label " + i);
			e.setProperty("intValue", i);
		}

		VertexQuery q = a.query();
		q.has("intValue", 3);
		q.direction(Direction.OUT);

		assertEquals(1, countResults((Iterator<String>) q.vertexIds()));

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testVertexQueryHas() {
		int num = 7;

		Vertex a = graph.addVertex("Vertex_a");
		Vertex b = graph.addVertex("Vertex_b");

		for (int i = 0; i < num; ++i) {
			Edge e = graph.addEdge("Edge_" + i, a, b, "label " + i);
			e.setProperty("intValue" + i, i);
		}

		VertexQuery q = a.query();
		q.has("intValue1");

		assertEquals(1, countResults((Iterator<String>) q.vertexIds()));
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

	private int countResults(Iterator<?> iter) {

		int count = 0;
		while (iter.hasNext()) {
			++count;
			iter.next();
		}

		return count;
	}
}
