package com.tinkerpop.blueprints.impls.arangodb.test;

import junit.framework.*;

public class AllTests {

	  public static Test suite() {
		    TestSuite suite = new TestSuite();
		    suite.addTestSuite(ArangoGraphTest.class);
		    suite.addTestSuite(ArangoVertexTest.class);
		    suite.addTestSuite(ArangoEdgeTest.class);
		    suite.addTestSuite(ArangoQueryTest.class);
		    suite.addTestSuite(ArangoDBGraphFactoryTest.class);
		    return suite;
		  }	
}
