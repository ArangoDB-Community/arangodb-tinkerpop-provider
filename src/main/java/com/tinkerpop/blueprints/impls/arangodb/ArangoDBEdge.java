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
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 */

public class ArangoDBEdge extends ArangoDBElement implements Edge
{
	/**
	 * the _from vertex 
	 */
	private Vertex outVertex = null;
	
	/**
	 * the _to vertex 
	 */
	private Vertex inVertex = null;
	
	static ArangoDBEdge create (ArangoDBGraph graph, Object id, Vertex outVertex, Vertex inVertex, String label) {
		String key = (id != null) ? id.toString() : null; 
		
		if (outVertex instanceof ArangoDBVertex && inVertex instanceof ArangoDBVertex) {
			ArangoDBVertex from = (ArangoDBVertex) outVertex;
			ArangoDBVertex to = (ArangoDBVertex) inVertex;
			
			try {
				ArangoDBSimpleEdge v = graph.client.createEdge(graph.getRawGraph(), key, label, from.getRawVertex(), to.getRawVertex(), null);
				return build(graph, v, outVertex, inVertex);
			} catch (ArangoDBException e) {
				if (e.errorNumber() == 1210) {
					throw ExceptionFactory.vertexWithIdAlreadyExists(id);
				}
				throw new IllegalArgumentException(e.getMessage());
			}								
		}
		throw new IllegalArgumentException("Wrong vertex class.");
		
	}
    
	static ArangoDBEdge load (ArangoDBGraph graph, Object id) {
		if (id == null) {
			throw ExceptionFactory.edgeIdCanNotBeNull();
		}
		
		String key = id.toString(); 

		ArangoDBEdge edge = graph.edgeCache.get(key);
		if (edge != null) {
			return edge;
		}
		
		try {
			ArangoDBSimpleEdge v = graph.client.getEdge(graph.getRawGraph(), key);
			return build(graph, v,  null ,null);			
		} catch (ArangoDBException e) {
			// do nothing
			return null;
		}		
	}
	
	static ArangoDBEdge build (ArangoDBGraph graph, ArangoDBSimpleEdge simpleEdge, Vertex outVertex, Vertex inVertex) {
		String id = simpleEdge.getDocumentKey();
		
		ArangoDBEdge vert = graph.edgeCache.get(id);
		if (vert != null) {
			vert.setDocument(simpleEdge);
			return vert;
		}
		
		ArangoDBEdge newEdge = new ArangoDBEdge(graph, simpleEdge, outVertex, inVertex);
		graph.edgeCache.put(newEdge.getRaw().getDocumentKey(), newEdge);
		return newEdge;							
	}

	private ArangoDBEdge(ArangoDBGraph graph, ArangoDBSimpleEdge edge, Vertex outVertex, Vertex inVertex) {
		this.graph = graph;
		this.document = edge;
		this.outVertex = outVertex;
		this.inVertex= inVertex;
	}
	
	public Vertex getVertex(Direction direction) throws IllegalArgumentException 
	{
        if (direction.equals(Direction.IN))
        {
        	if (inVertex == null) {
            	Object id = document.getProperty(ArangoDBSimpleEdge._TO);
            	inVertex = graph.getVertex(getKey(id));
        	}        	
    		return inVertex;
        }
        else if (direction.equals(Direction.OUT))
        {
        	if (outVertex == null) {
            	Object id = document.getProperty(ArangoDBSimpleEdge._FROM);
            	outVertex = graph.getVertex(getKey(id));        		
        	}
        	return outVertex;
        }
        else
        {
        	throw ExceptionFactory.bothIsNotSupported();
        }
	}
	
	private String getKey (Object id) {
		if (id == null) {			
			return "";
		}
		
		String[] parts = id.toString().split("/");
		
		if (parts.length > 1) {
			return parts[1];
		}
		
		return parts[0];
	}

	public String getLabel() 
	{
		Object l = document.getProperty(ArangoDBSimpleEdge._LABEL);
		if (l != null) {
			return l.toString();
		}
		
		return null;
	}
	
	public ArangoDBSimpleEdge getRawEdge () {
		return (ArangoDBSimpleEdge) document;
	}
	
	@Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
    
	public void remove () {
		if (document.isDeleted()) {
			return;
		}
		String key = document.getDocumentKey();
		try {
			graph.client.deleteEdge(graph.getRawGraph(), (ArangoDBSimpleEdge) document);
		} catch (ArangoDBException ex) {
			// ignore error;
		}
		graph.edgeCache.remove(key);
	}
    
	public void save () throws ArangoDBException {
		if (document.isDeleted()) {
			return;
		}
		graph.client.saveEdge(graph.getRawGraph(), (ArangoDBSimpleEdge) document);
	}
	
}
