package com.arangodb.blueprints.client.test;

import java.util.List;
import java.util.Vector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.arangodb.ArangoException;
import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.client.ArangoDBIndex;
import com.arangodb.blueprints.client.ArangoDBSimpleGraph;
import com.arangodb.entity.IndexType;

public class SimpleGraphIndexTest extends BaseTestCase {

	ArangoDBSimpleGraph graph = null;

	@Before
	public void setUp() {
		super.setUp();
		try {
			graph = client.createGraph(graphName, vertices, edges);

		} catch (ArangoException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}
	}

	@Test
	public void test_CreateIndex() throws ArangoDBException {
		Vector<String> fields = new Vector<String>();
		fields.add("name");
		ArangoDBIndex i = client.createVertexIndex(graph, IndexType.HASH, true, fields);
		Assert.assertNotNull(i);
	}

	@Test
	public void test_CreateIndexFails() throws ArangoDBException {
		Vector<String> fields = new Vector<String>();
		try {
			// empty field list
			client.createVertexIndex(graph, IndexType.HASH, true, fields);
			Assert.fail("ArangoDBException not thrown");
		} catch (ArangoDBException e) {

		}
	}

	@Test
	public void test_CreateIndexSkiplist() throws ArangoDBException {
		Vector<String> fields = new Vector<String>();
		fields.add("name");
		ArangoDBIndex i = client.createVertexIndex(graph, IndexType.SKIPLIST, false, fields);
		Assert.assertNotNull(i);
	}

	@Test
	public void test_CreateIndexSkiplistFails() {
		Vector<String> fields = new Vector<String>();
		try {
			client.createVertexIndex(graph, IndexType.SKIPLIST, false, fields);
			Assert.fail("ArangoDBException not thrown");
		} catch (ArangoDBException e) {
		}
	}

	@Test
	public void test_GetIndex() throws ArangoDBException {
		Vector<String> fields = new Vector<String>();
		fields.add("name");
		ArangoDBIndex i = client.createVertexIndex(graph, IndexType.HASH, true, fields);
		Assert.assertNotNull(i);

		ArangoDBIndex e = client.getIndex(i.getId());
		Assert.assertNotNull(e);
	}

	@Test
	public void test_DeleteIndex() throws ArangoDBException {
		Vector<String> fields = new Vector<String>();

		fields.add("name");
		ArangoDBIndex i = client.createVertexIndex(graph, IndexType.HASH, true, fields);
		Assert.assertNotNull(i);

		boolean deleted = client.deleteIndex(i.getId());
		Assert.assertTrue(deleted);

		ArangoDBIndex e = client.getIndex(i.getId());
		Assert.assertNull(e);
	}

	@Test
	public void test_GetIndices() throws ArangoDBException {
		Vector<String> fields = new Vector<String>();
		fields.add("name");
		client.createVertexIndex(graph, IndexType.HASH, true, fields);
		fields.clear();

		fields.add("street");
		client.createVertexIndex(graph, IndexType.HASH, true, fields);

		List<ArangoDBIndex> v = client.getVertexIndices(graph);
		Assert.assertEquals(3, v.size());
	}

}
