package com.arangodb.blueprints.client.test;

import org.junit.After;
import org.junit.Before;

import com.arangodb.ArangoException;
import com.arangodb.blueprints.client.ArangoDBConfiguration;
import com.arangodb.blueprints.client.ArangoDBSimpleGraphClient;

public abstract class BaseTestCase {

	protected ArangoDBSimpleGraphClient client;
	protected final String graphName = "test_graph1";
	protected final String vertices = "test_vertices1";
	protected final String edges = "test_edges1";

	@Before
	public void setUp() {

		// host name and port see: arangodb.properties
		ArangoDBConfiguration configuration = new ArangoDBConfiguration();

		client = new ArangoDBSimpleGraphClient(configuration);

		try {
			client.getDriver().deleteGraph(graphName);
		} catch (ArangoException e) {
		}

		try {
			client.getDriver().deleteCollection(vertices);
		} catch (ArangoException e) {
		}

		try {
			client.getDriver().deleteCollection(edges);
		} catch (ArangoException e) {
		}

	}

	@After
	public void tearDown() {
		try {
			client.getDriver().deleteCollection(vertices);
		} catch (ArangoException e) {
		}

		try {
			client.getDriver().deleteCollection(edges);
		} catch (ArangoException e) {
		}
		try {
			client.getDriver().deleteGraph(graphName);
		} catch (ArangoException e) {
		}

		client = null;
	}

}
