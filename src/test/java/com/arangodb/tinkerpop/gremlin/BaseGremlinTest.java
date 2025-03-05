package com.arangodb.tinkerpop.gremlin;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;

public abstract class BaseGremlinTest extends AbstractGremlinTest {

	protected ArangoDBGraph getGraph() {
		return (ArangoDBGraph) this.graph;
	}
}
