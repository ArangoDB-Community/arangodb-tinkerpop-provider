//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBException;
import org.apache.log4j.Logger;

import com.tinkerpop.blueprints.Vertex;

/**
 * The ArangoDB graph factory class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBGraphFactory {

	private static final String WEIGHT = "weight";
	private static final String KNOWS = "knows";
	private static final String CREATED = "created";
	/**
	 * the logger
	 */
	private static final Logger logger = Logger.getLogger(ArangoDBGraphFactory.class);

	private ArangoDBGraphFactory() {
		// this is a factory class
	}

	/**
	 * Static function to create a new ArangoDB graph.
	 * 
	 * Connects to ArangoDB database on localhost:8529.
	 * 
	 * @return the new graph
	 */
	public static ArangoDBGraph createArangoDBGraph() {
		return createArangoDBGraph("localhost", 8529);
	}

	/**
	 * Static function to create a new ArangoDB graph with example vertices and
	 * edges.
	 * 
	 * Connects to ArangoDB database on localhost:8529.
	 * 
	 * @return the new graph
	 */
	public static ArangoDBGraph createExampleArangoDBGraph() {
		return createExampleArangoDBGraph("localhost", 8529);
	}

	/**
	 * Static function to create a new ArangoDB graph.
	 * 
	 * @param host
	 *            Host name of the ArangoDB
	 * @param port
	 *            Port number of ArangoDB
	 * @return the new graph
	 */
	public static ArangoDBGraph createArangoDBGraph(String host, int port) {

		ArangoDBGraph graph = null;
		try {
			graph = new ArangoDBGraph(host, port, "factory_graph", "factory_vertices", "factory_edges");
		} catch (ArangoDBGraphException e) {
			logger.error("Could not get or create the graph.", e);
		}
		return graph;
	}

	/**
	 * Static function to create a new ArangoDB graph with example vertices and
	 * edges.
	 * 
	 * @param host
	 *            Host name of the ArangoDB
	 * @param port
	 *            Port number of ArangoDB
	 * @return the new graph
	 */
	public static ArangoDBGraph createExampleArangoDBGraph(String host, int port) {

		ArangoDBGraph graph = createArangoDBGraph(host, port);
		if (graph == null) {
			return null;
		}

		try {
			graph.getClient().truncateCollection(graph.getRawGraph().getVertexCollection());
		} catch (ArangoDBException e) {
			logger.error("Could not truncate vertices collection.", e);
		}

		try {
			graph.getClient().truncateCollection(graph.getRawGraph().getEdgeCollection());
		} catch (ArangoDBException e) {
			logger.error("Could not truncate edges collection.", e);
		}

		Vertex marko = graph.addVertex("1");
		marko.setProperty("name", "marko");
		marko.setProperty("age", 29);

		Vertex vadas = graph.addVertex("2");
		vadas.setProperty("name", "vadas");
		vadas.setProperty("age", 27);

		Vertex lop = graph.addVertex("3");
		lop.setProperty("name", "lop");
		lop.setProperty("lang", "java");

		Vertex josh = graph.addVertex("4");
		josh.setProperty("name", "josh");
		josh.setProperty("age", 32);

		Vertex ripple = graph.addVertex("5");
		ripple.setProperty("name", "ripple");
		ripple.setProperty("lang", "java");

		Vertex peter = graph.addVertex("6");
		peter.setProperty("name", "peter");
		peter.setProperty("age", 35);

		graph.addEdge("7", marko, vadas, KNOWS).setProperty(WEIGHT, 0.5f);
		graph.addEdge("8", marko, josh, KNOWS).setProperty(WEIGHT, 1.0f);
		graph.addEdge("9", marko, lop, CREATED).setProperty(WEIGHT, 0.4f);

		graph.addEdge("10", josh, ripple, CREATED).setProperty(WEIGHT, 1.0f);
		graph.addEdge("11", josh, lop, CREATED).setProperty(WEIGHT, 0.4f);

		graph.addEdge("12", peter, lop, CREATED).setProperty(WEIGHT, 0.2f);

		return graph;
	}

}
