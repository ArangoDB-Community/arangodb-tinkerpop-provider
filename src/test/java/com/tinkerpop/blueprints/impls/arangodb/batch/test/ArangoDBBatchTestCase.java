package com.tinkerpop.blueprints.impls.arangodb.batch.test;

import java.util.Iterator;

import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;

import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBEdge;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraph;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBVertex;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBConfiguration;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleGraphClient;

public abstract class ArangoDBBatchTestCase extends BaseTest {

	/**
	 * the client
	 */

	protected ArangoDBSimpleGraphClient tmpClient;

	/**
	 * the configuration
	 */

	protected ArangoDBConfiguration configuration;

	/**
	 * ArangoDB hostname
	 */

	protected final String host = "localhost";

	/**
	 * ArangoDB port
	 */

	protected final int port = 8529;

	/**
	 * name of the test graph
	 */

	protected final String graphName = "test_batch_graph1";

	/**
	 * name of the test vertex collection
	 */

	protected final String vertices = "test_batch_vertices1";

	/**
	 * name of the test edge collection
	 */

	protected final String edges = "test_batch_edges1";

	/**
	 * delete a graph in ArangoDB
	 * 
	 * @param name
	 *            the name of the graph
	 */
	protected void deleteGraph(String name) {
		try {
			tmpClient.deleteRequest("_api/document/_graphs/" + name);
		} catch (ArangoDBException e) {
		}
	}

	/**
	 * check for graph name
	 * 
	 * @param name
	 *            the name of the graph
	 * @return true if graph was found
	 */
	protected boolean hasGraph(String name) {
		try {
			JSONObject json = tmpClient.getRequest("_api/document/_graphs/" + name);
			if (json != null && !json.has("error")) {
				return true;
			}
		} catch (Exception e) {
		}
		return false;
	}

	/**
	 * delete a collection in ArangoDB
	 * 
	 * @param name
	 *            the name of the collection
	 */
	protected void deleteCollection(String name) {
		try {
			tmpClient.deleteRequest("_api/collection/" + name);
		} catch (ArangoDBException e) {
		}
	}

	/**
	 * checks the internal "_id" property in an ArangoDB vertex and edge object
	 * 
	 * @param element
	 *            the vertex or the edge object
	 * @return true, if the element has a none empty "_id" attribute
	 */
	protected boolean has_id(Element element) {

		if (element == null) {
			return false;
		}

		if (element.getClass().equals(ArangoDBVertex.class)) {
			ArangoDBVertex a = (ArangoDBVertex) element;

			if (a.getRaw().getProperty("_id") != null) {
				return true;
			}
		} else if (element.getClass().equals(ArangoDBEdge.class)) {
			ArangoDBEdge a = (ArangoDBEdge) element;

			if (a.getRaw().getProperty("_id") != null) {
				return true;
			}
		} else if (element.getClass().equals(ArangoDBGraph.class)) {
			ArangoDBGraph a = (ArangoDBGraph) element;
			if (a.getRawGraph().getProperty("_id") != null) {
				return true;
			}
		}

		return false;
	}

	/**
	 * checks for properties in an ArangoDB vertex and edge object
	 * 
	 * @param element
	 *            the vertex or the edge object
	 * @param key
	 *            the property name
	 * @param expects
	 *            the expected value of the attribute
	 * @return true, if the element has a none empty "_id" attribute
	 */
	protected boolean hasProperty(Element element, String key, Object expects) {

		if (element == null) {
			return false;
		}

		if (element.getClass().equals(ArangoDBVertex.class)) {
			ArangoDBVertex a = (ArangoDBVertex) element;

			if (expects.equals(a.getProperty(key))) {
				return true;
			}
		} else if (element.getClass().equals(ArangoDBEdge.class)) {
			ArangoDBEdge a = (ArangoDBEdge) element;

			if (expects.equals(a.getProperty(key))) {
				return true;
			}
		} else if (element.getClass().equals(ArangoDBGraph.class)) {
			ArangoDBGraph a = (ArangoDBGraph) element;

			if (expects.equals(a.getProperty(key))) {
				return true;
			}
		}

		return false;
	}

	/**
	 * count the number of elements
	 * 
	 * @param iter
	 *            the iterator
	 * @return the number of elements (or -1 for iter == null)
	 */
	protected int countElements(Iterator<?> iter) {
		if (iter == null) {
			return -1;
		}

		int count = 0;
		while (iter.hasNext()) {
			++count;
			iter.next();
		}
		return count;
	}

	protected int countElements(Iterable<?> iterable) {
		Iterator<?> iter = iterable.iterator();
		return countElements(iter);
	}

	@Before
	protected void setUp() {
		configuration = new ArangoDBConfiguration();
		configuration.setHost(host);
		configuration.setPort(port);

		tmpClient = new ArangoDBSimpleGraphClient(configuration);

		// delete graph
		deleteGraph(graphName);

		// delete test collections
		deleteCollection(edges);
		deleteCollection(vertices);
	}

	@After
	protected void tearDown() {
	}

}
