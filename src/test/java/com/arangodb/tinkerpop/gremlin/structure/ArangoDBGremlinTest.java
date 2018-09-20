package com.arangodb.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;

public class ArangoDBGremlinTest extends AbstractGremlinTest {

	protected ArangoDBGraph getGraph() {
		return (ArangoDBGraph) this.graph;
	}
}
