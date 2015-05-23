package com.tinkerpop.blueprints.impls.arangodb.client.test;

import org.junit.Assert;
import org.junit.Test;

import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;

public class ClientTest extends BaseTestCase {

	@Test
	public void test_ServerVersion() throws ArangoDBException {
		String version = client.getVersion();
		Assert.assertNotNull(version);
	}

}
