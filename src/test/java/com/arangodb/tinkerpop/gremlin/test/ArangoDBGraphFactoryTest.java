package com.arangodb.tinkerpop.gremlin.test;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphFactory;

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
