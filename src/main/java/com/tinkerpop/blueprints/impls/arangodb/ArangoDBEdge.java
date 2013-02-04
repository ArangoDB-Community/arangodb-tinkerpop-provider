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
	static ArangoDBEdge create (ArangoDBGraph graph, Object id, Vertex outVertex, Vertex inVertex, String label) {
		String key = (id != null) ? id.toString() : null; 
		
		if (outVertex instanceof ArangoDBVertex && inVertex instanceof ArangoDBVertex) {
			ArangoDBVertex from = (ArangoDBVertex) outVertex;
			ArangoDBVertex to = (ArangoDBVertex) inVertex;
			
			try {
				ArangoDBSimpleEdge v = graph.client.createEdge(graph.getRawGraph(), key, label, from.getRawVertex(), to.getRawVertex(), null);
				return build(graph, v);
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
			return build(graph, v);			
		} catch (ArangoDBException e) {
			// do nothing
			return null;
		}		
	}
	
	static ArangoDBEdge build (ArangoDBGraph graph, ArangoDBSimpleEdge simpleEdge) {
		String id = simpleEdge.getDocumentKey();
		
		ArangoDBEdge vert = graph.edgeCache.get(id);
		if (vert != null) {
			vert.setDocument(simpleEdge);
			return vert;
		}
		
		ArangoDBEdge newEdge = new ArangoDBEdge(graph, simpleEdge);
		graph.edgeCache.put(newEdge.getRaw().getDocumentKey(), newEdge);
		return newEdge;							
	}

	private Vertex inVertex = null;
	private Vertex outVertex = null;
	
	private ArangoDBEdge(ArangoDBGraph graph, ArangoDBSimpleEdge edge) {
		this.graph = graph;
		this.document = edge;
	}
	
	public Vertex getVertex(Direction direction) throws IllegalArgumentException 
	{
        if (direction.equals(Direction.IN))
        {
        	if (inVertex == null) {
            	Object id = document.getProperty(ArangoDBSimpleEdge._TO);
            	inVertex = graph.getVertex(id);
        	}        	
    		return inVertex;
        }
        else if (direction.equals(Direction.OUT))
        {
        	if (outVertex == null) {
            	Object id = document.getProperty(ArangoDBSimpleEdge._FROM);
            	outVertex = graph.getVertex(id);        		
        	}
        	return outVertex;
        }
        else
        {
        	throw ExceptionFactory.bothIsNotSupported();
        }
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
	
    public String toString() {
        return StringFactory.edgeString(this);
    }
    
	public void delete () {
		if (document.isDeleted()) {
			return;
		}
		String key = document.getDocumentKey();
		try {
			graph.client.deleteEdge(graph.getRawGraph(), (ArangoDBSimpleEdge) document);
		} catch (ArangoDBException e) {
		}			
		graph.edgeCache.remove(key);
		changed = false;
	}
    
	public void save () {
		if (document.isDeleted()) {
			return;
		}
		try {
			graph.client.saveEdge(graph.getRawGraph(), (ArangoDBSimpleEdge) document);
		} catch (ArangoDBException e) {
		}				
		changed = false;
	}
	
}
