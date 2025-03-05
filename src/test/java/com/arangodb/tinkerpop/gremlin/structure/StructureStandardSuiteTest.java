package com.arangodb.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = StructureGraphProvider.class, graph = ArangoDBGraph.class)
public class StructureStandardSuiteTest {

}
