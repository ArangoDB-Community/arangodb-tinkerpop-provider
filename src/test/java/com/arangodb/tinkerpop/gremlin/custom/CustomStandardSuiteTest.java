package com.arangodb.tinkerpop.gremlin.custom;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(CustomStandardSuite.class)
@GraphProviderClass(provider = CustomGraphProvider.class, graph = ArangoDBGraph.class)
public class CustomStandardSuiteTest {

}
