package com.tinkerpop.blueprints.impls.arangodb.client.test;

import junit.framework.*;

public class AllTests {

	  public static Test suite() {
		    TestSuite suite = new TestSuite();
		    suite.addTestSuite(ClientTest.class);
		    suite.addTestSuite(SimpleGraphTest.class);
		    suite.addTestSuite(SimpleVertexTest.class);
		    suite.addTestSuite(SimpleGraphVerticesTest.class);
		    suite.addTestSuite(SimpleVertexNeighborsTest.class);
		    
		    suite.addTestSuite(SimpleEdgeTest.class);
		    suite.addTestSuite(SimpleGraphEdgesTest.class);
		    
		    suite.addTestSuite(SimpleGraphIndexTest.class);

		    return suite;
		  }	
}
