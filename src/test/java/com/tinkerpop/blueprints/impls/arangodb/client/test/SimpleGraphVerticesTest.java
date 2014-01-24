package com.tinkerpop.blueprints.impls.arangodb.client.test;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBPropertyFilter;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBPropertyFilter.Compare;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleGraph;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleVertex;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleVertexCursor;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleVertexQuery;

public class SimpleGraphVerticesTest extends BaseTestCase {

	ArangoDBSimpleGraph graph = null;
	ArangoDBSimpleVertex vertex1 = null;
	ArangoDBSimpleVertex vertex2 = null;
	ArangoDBSimpleVertex vertex3 = null;
	ArangoDBSimpleVertex vertex4 = null;
	ArangoDBSimpleVertex vertex5 = null;

	protected void setUp() {
		super.setUp();
		try {
			JSONObject o = new JSONObject();
			graph = client.createGraph(graphName, vertices, edges);

			o.put("key1", 1);
			vertex1 = client.createVertex(graph, "v1", o);

			o.put("key1", 2);
			vertex2 = client.createVertex(graph, "v2", o);

			o.put("key1", 3);
			vertex3 = client.createVertex(graph, "v3", o);

			o.put("key1", 4);
			vertex4 = client.createVertex(graph, "v4", o);

			o.put("key1", 5);
			vertex5 = client.createVertex(graph, "v5", o);

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

	protected void tearDown() {
		super.tearDown();
	}

	public void test_getGraphVertices() {

		try {
			ArangoDBSimpleVertexQuery query = client.getGraphVertices(graph, null, null, false);
			assertNotNull(query);

			ArangoDBSimpleVertexCursor cursor = query.getResult();
			assertNotNull(cursor);

			int count = 0;

			while (cursor.hasNext()) {
				cursor.next();
				count++;
			}
			assertEquals(5, count);

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

	public void test_getGraphVerticesWithCount() {

		try {
			ArangoDBSimpleVertexQuery query = client.getGraphVertices(graph, null, null, true);
			assertNotNull(query);

			ArangoDBSimpleVertexCursor cursor = query.getResult();
			assertNotNull(cursor);

			assertEquals(5, cursor.count());

			int count = 0;

			while (cursor.hasNext()) {
				cursor.next();
				count++;
			}
			assertEquals(5, count);

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

	public void test_getGraphVerticesWithFilter() {

		try {
			ArangoDBPropertyFilter propfilter = new ArangoDBPropertyFilter();
			propfilter.has("key1", 2, Compare.GREATER_THAN);
			propfilter.has("key1", 5, Compare.LESS_THAN);

			ArangoDBSimpleVertexQuery query = client.getGraphVertices(graph, propfilter, null, false);
			assertNotNull(query);

			ArangoDBSimpleVertexCursor cursor = query.getResult();
			assertNotNull(cursor);

			int count = 0;

			while (cursor.hasNext()) {
				cursor.next();
				count++;
			}
			assertEquals(2, count);

		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

}
