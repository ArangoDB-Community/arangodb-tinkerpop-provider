package com.tinkerpop.blueprints.impls.arangodb.test;

import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraph;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraphFactory;

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
