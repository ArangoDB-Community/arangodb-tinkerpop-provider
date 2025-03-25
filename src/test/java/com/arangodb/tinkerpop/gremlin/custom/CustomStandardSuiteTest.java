package com.arangodb.tinkerpop.gremlin.custom;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(CustomStandardSuite.class)
@GraphProviderClass(provider = CustomGraphProvider.class, graph = CustomTestGraph.class)
public class CustomStandardSuiteTest {

}
