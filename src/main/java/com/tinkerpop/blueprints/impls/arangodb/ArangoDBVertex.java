//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb;

import com.tinkerpop.blueprints.impls.arangodb.client.*;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 */

public class ArangoDBVertex extends ArangoDBElement implements Vertex 
{	
	/**
     *  Creates a vertex
     *  
     *  @param graph                      a ArangoDBGraph
     *  @param id                         the id (key) of the vertex (can be null)
     *  
     *  @throws IllegalArgumentException  
     */
    
	static ArangoDBVertex create (ArangoDBGraph graph, Object id) {		
		String key = (id != null) ? id.toString() : null; 
				
		try {
			ArangoDBSimpleVertex v = graph.client.createVertex(graph.getRawGraph(), key, null);
			return build(graph, v);
		} catch (ArangoDBException e) {
			if (e.errorNumber() == 1210) {
				throw ExceptionFactory.vertexWithIdAlreadyExists(id);
			}
			throw new IllegalArgumentException(e.getMessage());
		}		
	}
	
	/**
     *  Creates a vertex by loading it 
     *  
     *  @param graph                      a ArangoDBGraph
     *  @param id                         the id (key) of the vertex (can be null)
     *  
     *  @throws IllegalArgumentException  
     */
    
	static ArangoDBVertex load (ArangoDBGraph graph, Object id) {	
		if (id == null) {
			throw ExceptionFactory.vertexIdCanNotBeNull();
		}
		
		String key = id.toString(); 

		ArangoDBVertex vert = graph.vertexCache.get(key);
		if (vert != null) {
			return vert;
		}
		
		try {			
			ArangoDBSimpleVertex v = graph.client.getVertex(graph.getRawGraph(), key);			
			return build(graph, v);
		} catch (ArangoDBException e) {
			// nothing found
			return null;
		}		
	}

	static ArangoDBVertex build (ArangoDBGraph graph, ArangoDBSimpleVertex simpleVertex) {
		String id = simpleVertex.getDocumentKey();
		
		ArangoDBVertex vert = graph.vertexCache.get(id);
		if (vert != null) {
			vert.setDocument(simpleVertex);
			return vert;
		}
		
		ArangoDBVertex newVertex = new ArangoDBVertex(graph, simpleVertex);
		graph.vertexCache.put(newVertex.getRaw().getDocumentKey(), newVertex);
		return newVertex;							
	}
	
	
	private ArangoDBVertex(ArangoDBGraph graph, ArangoDBSimpleVertex vertex) 
	{
		this.graph = graph;
		this.document = vertex;
	}

	public Iterable<Edge> getEdges(Direction direction, String... labels) {
		if (document.isDeleted()) {
			return null;
		}
		ArangoDBVertexQuery q = new ArangoDBVertexQuery(graph, this);
		q.direction(direction);
		q.labels(labels);
				
		return q.edges();
	}

	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
		if (document.isDeleted()) {
			return null;
		}
		ArangoDBVertexQuery q = new ArangoDBVertexQuery(graph, this);
		q.direction(direction);
		q.labels(labels);
				
		return q.vertices();
	}

	public VertexQuery query() {
		if (document.isDeleted()) {
			return null;
		}
		return new ArangoDBVertexQuery(graph, this);
	}

	public ArangoDBSimpleVertex getRawVertex () {
		return (ArangoDBSimpleVertex) document;
	}
	
    public String toString() {
        return StringFactory.vertexString(this);
    }
	
	public void remove () {
		if (document.isDeleted()) {
			return;
		}
		String key = document.getDocumentKey();
		try {
			graph.client.deleteVertex(graph.getRawGraph(), (ArangoDBSimpleVertex) document);
		} catch (ArangoDBException ex) {
			// ignore error
		}
		graph.vertexCache.remove(key);
	}
	
	public void save () throws ArangoDBException {
		if (document.isDeleted()) {
			return;
		}
		graph.client.saveVertex(graph.getRawGraph(), (ArangoDBSimpleVertex) document);
	}

	public Edge addEdge(String label, Vertex inVertex) {
		return ArangoDBEdge.create(this.graph, null, this, inVertex, label);
	}

}
