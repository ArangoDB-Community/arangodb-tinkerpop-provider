//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;

public class ArangoDBGraphFactory {

    public static ArangoDBGraph createArangoDBGraph() {
    	return createArangoDBGraph("localhost", 8529);
    }
	
    public static ArangoDBGraph createArangoDBGraph(String host, int port) {
    	
    	ArangoDBGraph graph = null;
		try {
			graph = new ArangoDBGraph(host, port, "factory_graph", "factory_vertices", "factory_edges");
		} catch (ArangoDBGraphException e) {
			e.printStackTrace();
			System.out.println("Could not get or create the graph.");
			return null;
		}

    	try {
    		graph.client.putRequest("_api/collection/" + graph.getRawGraph().getVertexCollection() + "/truncate", null);
		} catch (ArangoDBException e) {
			System.out.println("Could not truncate vertices collection.");
		}
		
    	try {
    		graph.client.putRequest("_api/collection/" + graph.getRawGraph().getEdgeCollection() + "/truncate", null);
		} catch (ArangoDBException e) {
			System.out.println("Could not truncate edges collection.");
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

        graph.addEdge("7", marko, vadas, "knows").setProperty("weight", 0.5f);
        graph.addEdge("8", marko, josh, "knows").setProperty("weight", 1.0f);
        graph.addEdge("9", marko, lop, "created").setProperty("weight", 0.4f);

        graph.addEdge("10", josh, ripple, "created").setProperty("weight", 1.0f);
        graph.addEdge("11", josh, lop, "created").setProperty("weight", 0.4f);

        graph.addEdge("12", peter, lop, "created").setProperty("weight", 0.2f);

		return graph;
    }

}
