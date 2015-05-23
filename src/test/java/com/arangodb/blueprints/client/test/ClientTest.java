package com.arangodb.blueprints.client.test;

import org.junit.Assert;
import org.junit.Test;

import com.arangodb.blueprints.client.ArangoDBException;

public class ClientTest extends BaseTestCase {

	@Test
	public void test_ServerVersion() throws ArangoDBException {
		String version = client.getVersion();
		Assert.assertNotNull(version);
	}

}
