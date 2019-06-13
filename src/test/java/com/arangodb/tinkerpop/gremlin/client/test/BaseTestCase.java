package com.arangodb.tinkerpop.gremlin.client.test;

import java.util.Properties;

import com.arangodb.tinkerpop.gremlin.client.EssentialArangoDatabase;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Before;

public abstract class BaseTestCase {

	protected EssentialArangoDatabase client;
	protected final String graphName = "test_graph1";
	protected final String vertices = "test_vertices1";
	protected final String edges = "test_edges1";

	@Before
	public void setUp() throws Exception {

		// host name and port see: arangodb.properties
		PropertiesConfiguration configuration = new PropertiesConfiguration();
		configuration.setProperty("arangodb.hosts", "127.0.0.1:8529");
		configuration.setProperty("arangodb.user", "gremlin");
		configuration.setProperty("arangodb.password", "gremlin");
		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
		
		client = new EssentialArangoDatabase(arangoProperties, "tinkerpop", true, null);
		
		client.deleteGraph(graphName);
		client.deleteCollection(vertices);
		client.deleteCollection(edges);
		
	}

	@After
	public void tearDown() {
		
		client.deleteCollection(vertices);
		client.deleteCollection(edges);
		client.deleteGraph(graphName);
		client = null;
	}

}
