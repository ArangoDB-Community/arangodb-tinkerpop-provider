package com.tinkerpop.blueprints.impls.arangodb.client.test;

import java.util.List;
import java.util.Vector;

import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBIndex;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleGraph;

public class SimpleGraphIndexTest extends BaseTestCase {

	ArangoDBSimpleGraph graph = null;
	
	protected void setUp() {
		super.setUp();
		try {			
			graph = client.createGraph(graphName, vertices, edges);
						
		} catch (ArangoDBException e) {
			e.printStackTrace();
			assertTrue(false);		
		}
	}

	protected void tearDown() {
		super.tearDown();
	}

	public void test_CreateIndex () {
		Vector<String> fields = new Vector<String>();
		
		try {
			// fails
			client.createVertexIndex(graph, "hash", true, fields);
			assertTrue(false);		
		} catch (ArangoDBException e) {			
		}

		try {
			fields.add("name");
			ArangoDBIndex i = client.createVertexIndex(graph, "hash", true, fields);
			assertNotNull(i);						
		} catch (ArangoDBException e) {
			assertTrue(false);		
		}		
	}

	public void test_GetIndex () {
		Vector<String> fields = new Vector<String>();
		
		try {
			fields.add("name");
			ArangoDBIndex i = client.createVertexIndex(graph, "hash", true, fields);
			assertNotNull(i);
			
			ArangoDBIndex e = client.getIndex(i.getId());
			assertNotNull(e);
			
		} catch (ArangoDBException e) {
			assertTrue(false);		
		}		
	}

	public void test_DeleteIndex () {
		Vector<String> fields = new Vector<String>();
		
		try {
			fields.add("name");
			ArangoDBIndex i = client.createVertexIndex(graph, "hash", true, fields);
			assertNotNull(i);
			
			boolean deleted = client.deleteIndex(i.getId());
			assertTrue(deleted);
			
			ArangoDBIndex e = client.getIndex(i.getId());
			assertNull(e);
			
		} catch (ArangoDBException e) {
			assertTrue(false);		
		}		
	}

	public void test_GetIndices () {
		Vector<String> fields = new Vector<String>();
		
		try {
			fields.add("name");
			client.createVertexIndex(graph, "hash", true, fields);
			fields.clear();
			
			fields.add("street");
			client.createVertexIndex(graph, "hash", true, fields);
			
			List<ArangoDBIndex> v = client.getVertexIndices(graph);
			assertEquals(3, v.size());
			
		} catch (ArangoDBException e) {
			assertTrue(false);		
		}		
	}
	
}
