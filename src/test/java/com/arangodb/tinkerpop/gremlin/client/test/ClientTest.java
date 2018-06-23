package com.arangodb.tinkerpop.gremlin.client.test;

import org.junit.Assert;
import org.junit.Test;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBException;

public class ClientTest extends BaseTestCase {

	@Test
	public void test_ServerVersion() throws ArangoDBException {
		String version = client.getVersion();
		Assert.assertNotNull(version);
	}

}
