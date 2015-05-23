package com.tinkerpop.blueprints.impls.arangodb.client.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.arangodb.ArangoException;
import com.arangodb.CursorResult;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBBaseQuery;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBPropertyFilter;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBPropertyFilter.Compare;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleEdge;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleGraph;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleVertex;

public class SimpleGraphEdgesTest extends BaseTestCase {

	ArangoDBSimpleGraph graph = null;
	ArangoDBSimpleVertex vertex1 = null;
	ArangoDBSimpleVertex vertex2 = null;
	ArangoDBSimpleVertex vertex3 = null;
	ArangoDBSimpleVertex vertex4 = null;
	ArangoDBSimpleVertex vertex5 = null;

	ArangoDBSimpleEdge edge1 = null;
	ArangoDBSimpleEdge edge2 = null;
	ArangoDBSimpleEdge edge3 = null;
	ArangoDBSimpleEdge edge4 = null;

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

			o = new HashMap<String, Object>();
			o.put("edgeKey1", 1);
			edge1 = client.createEdge(graph, "edge1", "label1", vertex1, vertex2, o);

			o = new HashMap<String, Object>();
			o.put("edgeKey1", 2);
			edge2 = client.createEdge(graph, "edge2", "label2", vertex2, vertex3, o);

			o = new HashMap<String, Object>();
			o.put("edgeKey1", 3);
			edge3 = client.createEdge(graph, "edge3", "label3", vertex3, vertex4, o);

			o = new HashMap<String, Object>();
			o.put("edgeKey1", 4);
			edge4 = client.createEdge(graph, "edge4", "label4", vertex4, vertex5, o);

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
	public void test_getGraphEdges() throws ArangoDBException {
		ArangoDBBaseQuery query = client.getGraphEdges(graph, null, null, null, false);
		Assert.assertNotNull(query);

		CursorResult<Map> cursor = query.getCursorResult();
		Assert.assertNotNull(cursor);

		int count = 0;
		Iterator<Map> iterator = cursor.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			count++;
		}
		Assert.assertEquals(4, count);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void test_getGraphEdgesCount() throws ArangoDBException {

		ArangoDBBaseQuery query = client.getGraphEdges(graph, null, null, null, true);
		Assert.assertNotNull(query);

		CursorResult<Map> cursor = query.getCursorResult();
		Assert.assertNotNull(cursor);

		Assert.assertEquals(4, cursor.getCount());
		int count = 0;

		Iterator<Map> iterator = cursor.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			count++;
		}
		Assert.assertEquals(4, count);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void test_getGraphVerticesWithPropertyFilter() throws ArangoDBException {
		ArangoDBPropertyFilter propfilter = new ArangoDBPropertyFilter();
		propfilter.has("edgeKey1", 2, Compare.GREATER_THAN);
		propfilter.has("edgeKey1", 5, Compare.LESS_THAN);

		ArangoDBBaseQuery query = client.getGraphEdges(graph, propfilter, null, null, false);
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

	@SuppressWarnings("rawtypes")
	@Test
	public void test_getGraphVerticesWithLabelFilter() throws ArangoDBException {
		Vector<String> labels = new Vector<String>();
		labels.add("label2");

		ArangoDBBaseQuery query = client.getGraphEdges(graph, null, labels, null, false);
		Assert.assertNotNull(query);

		CursorResult<Map> cursor = query.getCursorResult();
		Assert.assertNotNull(cursor);

		int count = 0;

		Iterator<Map> iterator = cursor.iterator();
		while (iterator.hasNext()) {
			@SuppressWarnings("unchecked")
			ArangoDBSimpleEdge e = new ArangoDBSimpleEdge(iterator.next());
			Assert.assertEquals("label2", e.getLabel());
			count++;
		}
		Assert.assertEquals(1, count);
	}

}
