//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoGraph;
import com.arangodb.entity.GraphEntity;

/**
 * The arangodb graph class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */
@Deprecated
public class ArangoDBSimpleGraph {

	private GraphEntity graphEntity;
	private String vertexCollectionName;
	private String edgeCollectionName;

	/**
	 * Creates a graph by a ArangoDB GraphEntity
	 * 
	 * @param graphEntity
	 *            The ArangoDB GraphEntity
	 */
	public ArangoDBSimpleGraph(GraphEntity graphEntity, String vertexCollectionName, String edgeCollectionName) {
		this.graphEntity = graphEntity;
		this.vertexCollectionName = vertexCollectionName;
		this.edgeCollectionName = edgeCollectionName;
	}

	public ArangoDBSimpleGraph(ArangoGraph graph) {
		// TODO Auto-generated constructor stub
		GraphEntity e = graph.getInfo();
		
	}

	/**
	 * Returns the name of the graph
	 * 
	 * @return the name of the graph
	 */
	public String getName() {
		return graphEntity.getName();
	}

	/**
	 * Returns the name of the edge collection
	 * 
	 * @return the name of the edge collection
	 */
	public String getEdgeCollection() {
		return edgeCollectionName;
	}

	/**
	 * Returns the name of the vertex collection
	 * 
	 * @return the name of the vertex collection
	 */
	public String getVertexCollection() {
		return vertexCollectionName;
	}

	/**
	 * Returns the GraphEntity object
	 * 
	 * @return the GraphEntity object
	 */
	public GraphEntity getGraphEntity() {
		return graphEntity;
	}

	@Override
	public String toString() {
		return "{\"name\":\"" + getName() + "\",\"vertices\":\"" + getVertexCollection() + "\",\"edges\":\""
				+ getEdgeCollection() + "\"}";
	}

}
