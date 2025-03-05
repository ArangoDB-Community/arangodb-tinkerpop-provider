package com.arangodb.tinkerpop.gremlin.process;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = ProcessGraphProvider.class, graph = ArangoDBGraph.class)
public class ProcessStandardSuiteTest {

}
