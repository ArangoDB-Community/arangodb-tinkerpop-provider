package com.arangodb.blueprints.benchmark;

import java.lang.reflect.Method;

import com.arangodb.ArangoException;
import com.arangodb.blueprints.ArangoDBGraph;
import com.arangodb.blueprints.ArangoDBGraphException;
import com.arangodb.blueprints.client.ArangoDBConfiguration;
import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.client.ArangoDBSimpleGraphClient;
import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexQueryTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public class BenchmarkTest extends GraphTest {

	public static final String ARANGODB_HOSTNAME = "localhost";
	public static final int ARANGODB_PORT = 8529;
	public static final String NAME_GRAPH = "BlueprintsBenchmarkGraph";
	public static final String NAME_VERTEX_COLLECTION = "BenchmarkVertices";
	public static final String NAME_EDGE_COLLECTION = "BenchmarkEdges";
	public static final String NAME_DATABASE = "BlueprintsBenchmark";

	private ArangoDBConfiguration configuration;
	private ArangoDBSimpleGraphClient client;

	public BenchmarkTest() {
		configuration = new ArangoDBConfiguration(ARANGODB_HOSTNAME, ARANGODB_PORT);
		configuration.setDefaultDatabase(NAME_DATABASE);
		client = new ArangoDBSimpleGraphClient(configuration);
		try {
			client.getDriver().createDatabase(configuration.getDefaultDatabase());
		} catch (ArangoException e) {
		}
		deleteGraph(NAME_GRAPH);
		deleteCollection(NAME_EDGE_COLLECTION);
		deleteCollection(NAME_VERTEX_COLLECTION);

	}

	// special test
	// public void testArangoDbBenchmarkTestSuite() throws Exception {
	// this.stopWatch();
	// doTestSuite(new ArangoDbBenchmarkTestSuite(this));
	// printTestPerformance("VertexTestSuite", this.stopWatch());
	// }

	//
	// blueprints tests
	//

	// public void testVertexTestSuite() throws Exception {
	// this.stopWatch();
	// doTestSuite(new VertexTestSuite(this));
	// printTestPerformance("VertexTestSuite", this.stopWatch());
	// }

	public void testEdgeTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new EdgeTestSuite(this));
		printTestPerformance("EdgeTestSuite", this.stopWatch());
	}

	public void testGraphTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new GraphTestSuite(this));
		printTestPerformance("GraphTestSuite", this.stopWatch());
	}

	public void testVertexQueryTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new VertexQueryTestSuite(this));
		printTestPerformance("VertexQueryTestSuite", this.stopWatch());
	}

	// public void testGraphQueryTestSuite() throws Exception {
	// this.stopWatch();
	// doTestSuite(new GraphQueryTestSuite(this));
	// printTestPerformance("GraphQueryTestSuite", this.stopWatch());
	// }

	// public void testGraphMLReaderTestSuite() throws Exception {
	// this.stopWatch();
	// doTestSuite(new GraphMLReaderTestSuite(this));
	// printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
	// }

	// public void testGraphSONReaderTestSuite() throws Exception {
	// this.stopWatch();
	// doTestSuite(new GraphSONReaderTestSuite(this));
	// printTestPerformance("GraphSONReaderTestSuite", this.stopWatch());
	// }

	// public void testGMLReaderTestSuite() throws Exception {
	// this.stopWatch();
	// doTestSuite(new GMLReaderTestSuite(this));
	// printTestPerformance("GMLReaderTestSuite", this.stopWatch());
	// }

	@Override
	public void doTestSuite(TestSuite testSuite) throws Exception {

		truncateCollections();

		for (Method method : testSuite.getClass().getDeclaredMethods()) {
			if (method.getName().startsWith("test")) {
				System.out.println("Testing " + method.getName() + "...");
				method.invoke(testSuite);

				truncateCollections();
			}
		}
	}

	@Override
	public Graph generateGraph() {
		return generateGraph(NAME_GRAPH);
	}

	@Override
	public Graph generateGraph(String graphName) {
		try {
			return new ArangoDBGraph(configuration, graphName, NAME_VERTEX_COLLECTION, NAME_EDGE_COLLECTION);
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			return null;
		}
	}

	//
	// private functions
	//

	private void deleteGraph(String name) {
		try {
			client.getDriver().deleteGraph(name);
		} catch (ArangoException e) {
		}
	}

	private void deleteCollection(String name) {
		try {
			client.getDriver().deleteCollection(name);
		} catch (ArangoException e) {
		}
	}

	private void truncateCollections() {
		try {
			client.truncateCollection(NAME_EDGE_COLLECTION);
		} catch (ArangoDBException e) {
		}
		try {
			client.truncateCollection(NAME_VERTEX_COLLECTION);
		} catch (ArangoDBException e) {
		}
	}

}
