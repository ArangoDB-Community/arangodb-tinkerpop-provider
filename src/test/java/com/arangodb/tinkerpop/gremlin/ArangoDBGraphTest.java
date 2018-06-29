package com.arangodb.tinkerpop.gremlin;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

@RunWith(ArangoDBTestSuite.class)
@GraphProviderClass(provider = ArangoDBGraphProvider.class, graph = ArangoDBGraph.class)
public class ArangoDBGraphTest {

}
