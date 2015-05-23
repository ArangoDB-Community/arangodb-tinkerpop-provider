package com.arangodb.blueprints.test;

import com.arangodb.blueprints.ArangoDBGraph;
import com.arangodb.blueprints.ArangoDBGraphFactory;

public class ArangoDBGraphFactoryTest extends ArangoDBTestCase {
		
	public void testCreateGraph() {
		ArangoDBGraph graph = ArangoDBGraphFactory.createArangoDBGraph();		
		assertNotNull(graph);
		graph.shutdown();

		ArangoDBGraph graph2 = ArangoDBGraphFactory.createArangoDBGraph();		
		assertNotNull(graph2);
		graph2.shutdown();		
	}

}
