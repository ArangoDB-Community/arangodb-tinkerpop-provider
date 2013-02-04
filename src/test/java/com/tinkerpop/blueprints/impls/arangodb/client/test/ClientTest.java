package com.tinkerpop.blueprints.impls.arangodb.client.test;

import com.tinkerpop.blueprints.impls.arangodb.client.*;

public class ClientTest extends BaseTestCase {
	
	protected void setUp() {
		super.setUp();
	}

	protected void tearDown() {
		super.tearDown();
	}

	public void test_ServerVersion () {
		
		String version = null;
		try {
			version = client.getVersion();
		} catch (ArangoDBException e) {
			e.printStackTrace();
		}
		
		assertNotNull(version);		
	}

}
