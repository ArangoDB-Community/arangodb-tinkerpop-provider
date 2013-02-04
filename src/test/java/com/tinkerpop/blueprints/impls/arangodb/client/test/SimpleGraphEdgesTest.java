package com.tinkerpop.blueprints.impls.arangodb.client.test;

import java.util.Vector;

import com.tinkerpop.blueprints.impls.arangodb.client.*;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBPropertyFilter.Compare;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


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
			
			o.put("edgeKey1", 1);
			edge1 = client.createEdge(graph, "edge1", "label1", vertex1, vertex2, o);

			o.put("edgeKey1", 2);
			edge2 = client.createEdge(graph, "edge2", "label2", vertex2, vertex3, o);

			o.put("edgeKey1", 3);
			edge3 = client.createEdge(graph, "edge3", "label3", vertex3, vertex4, o);

			o.put("edgeKey1", 4);
			edge4 = client.createEdge(graph, "edge4", "label4", vertex4, vertex5, o);

						
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

	public void test_getGraphEdges () {
		
		try {
			ArangoDBSimpleEdgeQuery query = client.getGraphEdges(graph, null, null, false);
			assertNotNull(query);		
			
			ArangoDBSimpleEdgeCursor cursor = query.getResult();
			assertNotNull(cursor);
			
			int count = 0;
			
			while (cursor.hasNext()) {
				cursor.next();
				count++;
			}
			assertEquals(4, count);		
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}		
	}

	public void test_getGraphEdgesCount () {
		
		try {
			ArangoDBSimpleEdgeQuery query = client.getGraphEdges(graph, null, null, true);
			assertNotNull(query);		
			
			ArangoDBSimpleEdgeCursor cursor = query.getResult();
			assertNotNull(cursor);
			
			assertEquals(4, cursor.count());		
			int count = 0;
			
			while (cursor.hasNext()) {
				cursor.next();
				count++;
			}
			assertEquals(4, count);		
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}		
	}
	

	public void test_getGraphVerticesWithPropertyFilter () {
		
		try {
			ArangoDBPropertyFilter propfilter = new ArangoDBPropertyFilter();
			propfilter.has("edgeKey1", 2, Compare.GREATER_THAN);
			propfilter.has("edgeKey1", 5, Compare.LESS_THAN);
			
			ArangoDBSimpleEdgeQuery query = client.getGraphEdges(graph, propfilter, null, false);
			assertNotNull(query);		
			
			ArangoDBSimpleEdgeCursor cursor = query.getResult();
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

	public void test_getGraphVerticesWithLabelFilter () {
		
		try {
			Vector<String> labels = new Vector<String>();
			labels.add("label2");
			
			ArangoDBSimpleEdgeQuery query = client.getGraphEdges(graph, null, labels, false);
			assertNotNull(query);		
			
			ArangoDBSimpleEdgeCursor cursor = query.getResult();
			assertNotNull(cursor);
			
			int count = 0;
			
			while (cursor.hasNext()) {
				ArangoDBSimpleEdge e = cursor.next();
				assertEquals("label2", e.getLabel());				
				count++;
			}
			assertEquals(1, count);		
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}		
	}
	
}
