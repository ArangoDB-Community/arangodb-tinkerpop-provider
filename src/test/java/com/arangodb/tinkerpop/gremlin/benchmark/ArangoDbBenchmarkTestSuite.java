package com.arangodb.tinkerpop.gremlin.benchmark;

import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

public class ArangoDbBenchmarkTestSuite extends TestSuite {

	private static final int TOTAL_RUNS = 10;

	public ArangoDbBenchmarkTestSuite() {
	}

	public ArangoDbBenchmarkTestSuite(final GraphTest graphTest) {
		super(graphTest);
	}

	public void testArangoDbGraph() throws Exception {
		double totalTime = 0.0d;
		Graph graph = graphTest.generateGraph();
		GraphMLReader.inputGraph(graph, GraphMLReader.class.getResourceAsStream("graph-example-2.xml"));
		graph.shutdown();

		for (int i = 0; i < TOTAL_RUNS; i++) {
			graph = graphTest.generateGraph();
			this.stopWatch();
			int counter = 0;
			Iterable<Vertex> vv = graph.getVertices();
			for (final Vertex vertex : vv) {
				counter++;
				Iterable<Edge> ee = vertex.getEdges(Direction.OUT);
				for (final Edge edge : ee) {
					counter++;
					final Vertex vertex2 = edge.getVertex(Direction.IN);
					counter++;
					Iterable<Edge> ee2 = vertex2.getEdges(Direction.OUT);
					for (final Edge edge2 : ee2) {
						counter++;
						final Vertex vertex3 = edge2.getVertex(Direction.IN);
						counter++;
						Iterable<Edge> ee3 = vertex3.getEdges(Direction.OUT);
						for (final Edge edge3 : ee3) {
							counter++;
							edge3.getVertex(Direction.OUT);
							counter++;
						}

					}

				}

			}

			double currentTime = this.stopWatch();
			totalTime = totalTime + currentTime;
			BaseTest.printPerformance(graph.toString(), counter, "ArangoDB elements touched (run=" + i + ")",
				currentTime);
			graph.shutdown();
		}
		BaseTest.printPerformance("ArangoDB", 1, "ArangoDB experiment average", totalTime / (double) TOTAL_RUNS);
	}
}
