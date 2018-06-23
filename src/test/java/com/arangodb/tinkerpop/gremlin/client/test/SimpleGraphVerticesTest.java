package com.arangodb.tinkerpop.gremlin.client.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.arangodb.ArangoException;
import com.arangodb.CursorResult;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBSimpleGraph;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBSimpleVertex;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter.Compare;

public class SimpleGraphVerticesTest extends BaseTestCase {

	ArangoDBSimpleGraph graph = null;
	ArangoDBSimpleVertex vertex1 = null;
	ArangoDBSimpleVertex vertex2 = null;
	ArangoDBSimpleVertex vertex3 = null;
	ArangoDBSimpleVertex vertex4 = null;
	ArangoDBSimpleVertex vertex5 = null;

	@Before
	public void setUp() {
		super.setUp();
		try {
			Map<String, Object> o = new HashMap<String, Object>();
			graph = client.createGraph(graphName, vertices, edges);

			o.put("key1", 1);
			vertex1 = client.createVertex(graph, "v1", o);

			o = new HashMap<String, Object>();
			o.put("key1", 2);
			vertex2 = client.createVertex(graph, "v2", o);

			o = new HashMap<String, Object>();
			o.put("key1", 3);
			vertex3 = client.createVertex(graph, "v3", o);

			o = new HashMap<String, Object>();
			o.put("key1", 4);
			vertex4 = client.createVertex(graph, "v4", o);

			o = new HashMap<String, Object>();
			o.put("key1", 5);
			vertex5 = client.createVertex(graph, "v5", o);

		} catch (ArangoDBException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		} catch (ArangoException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}
	}

	@After
	public void tearDown() {
		super.tearDown();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void test_getGraphVertices() throws ArangoDBException {
		ArangoDBQuery query = client.getGraphVertices(graph, null, null, false);
		Assert.assertNotNull(query);

		CursorResult<Map> cursor = query.getCursorResult();
		Assert.assertNotNull(cursor);

		int count = 0;
		Iterator<Map> iterator = cursor.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			count++;
		}
		Assert.assertEquals(5, count);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void test_getGraphVerticesWithCount() throws ArangoDBException {
		ArangoDBQuery query = client.getGraphVertices(graph, null, null, true);
		Assert.assertNotNull(query);

		CursorResult<Map> cursor = query.getCursorResult();
		Assert.assertNotNull(cursor);

		Assert.assertEquals(5, cursor.getCount());

		int count = 0;
		Iterator<Map> iterator = cursor.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			count++;
		}
		Assert.assertEquals(5, count);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void test_getGraphVerticesWithFilter() throws ArangoDBException {
		ArangoDBPropertyFilter propfilter = new ArangoDBPropertyFilter();
		propfilter.has("key1", 2, Compare.GREATER_THAN);
		propfilter.has("key1", 5, Compare.LESS_THAN);

		ArangoDBQuery query = client.getGraphVertices(graph, propfilter, null, false);
		Assert.assertNotNull(query);

		CursorResult<Map> cursor = query.getCursorResult();
		Assert.assertNotNull(cursor);

		int count = 0;
		Iterator<Map> iterator = cursor.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			count++;
		}
		Assert.assertEquals(2, count);
	}

}
