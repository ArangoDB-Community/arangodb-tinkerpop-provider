package com.tinkerpop.blueprints.impls.arangodb.client.test;

import junit.framework.*;

import com.tinkerpop.blueprints.impls.arangodb.client.*;


public abstract class BaseTestCase extends TestCase {

    /**
     * the client 
     */

	protected ArangoDBSimpleGraphClient client;
	
    /**
     * name of the test graph 
     */

	protected final String graphName = "test_graph1";
	
    /**
     * name of the test vertex collection 
     */

	protected final String vertices = "test_vertices1";
	
    /**
     * name of the test edge collection 
     */

	protected final String edges = "test_edges1";
	
	protected void setUp() {
		ArangoDBConfiguration configuration = new ArangoDBConfiguration();
	
		client = new ArangoDBSimpleGraphClient(configuration);
		
		try {
			client.putRequest("_api/collection/_graphs/truncate", null);
		} catch (ArangoDBException e) {
			e.printStackTrace();
		}
		
		try {
			client.deleteRequest("_api/collection/" + vertices);
		} catch (ArangoDBException e) {
		}
		
		try {
			client.deleteRequest("_api/collection/" + edges);
		} catch (ArangoDBException e) {
		}
		
	}

	protected void tearDown() {
		try {
			client.deleteRequest("_api/collection/" + vertices);
		} catch (ArangoDBException e) {
		}
		
		try {
			client.deleteRequest("_api/collection/" + edges);
		} catch (ArangoDBException e) {
		}

                client = null;
	}


}
