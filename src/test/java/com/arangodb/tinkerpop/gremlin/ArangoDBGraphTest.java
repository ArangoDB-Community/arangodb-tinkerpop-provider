package com.arangodb.tinkerpop.gremlin;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = ArangoDBGraphProvider.class, graph = ArangoDBGraph.class)
public class ArangoDBGraphTest {

}
