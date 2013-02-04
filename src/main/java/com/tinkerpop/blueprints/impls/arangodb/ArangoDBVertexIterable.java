//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.tinkerpop.blueprints.impls.arangodb.client.*;

import com.tinkerpop.blueprints.Vertex;

/**
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 */

public class ArangoDBVertexIterable<T extends Vertex> implements Iterable<ArangoDBVertex> {
	
    private final ArangoDBGraph graph;
    private final ArangoDBSimpleVertexQuery query;

    public ArangoDBVertexIterable(final ArangoDBGraph graph, final ArangoDBSimpleVertexQuery query) {
        this.graph = graph;
        this.query = query;
    }

    public Iterator<ArangoDBVertex> iterator() {
    	
    	return new Iterator<ArangoDBVertex>() {

    		private ArangoDBSimpleVertexCursor cursor;

    		{
    			try {
    				// save changed edges and vertices
    				graph.save();
    				
    				if (query == null) {
    					// create a dummy cursor
        				cursor = new ArangoDBSimpleVertexCursor(graph.client, null);    					
    				}
    				else {
        				cursor = query.getResult();    					
    				}
    			} catch (ArangoDBException e) {
    				cursor = new ArangoDBSimpleVertexCursor(graph.client, null);
    			}    
    		}    		

    		public boolean hasNext() {
    			return cursor.hasNext();
    		}

    		public ArangoDBVertex next() {
    			if (!cursor.hasNext()) {
    				throw new NoSuchElementException();
    			}
    			
    			ArangoDBSimpleVertex simpleVertex = cursor.next();

    			if (simpleVertex == null) {
    				return null;
    			}
    			
    			return ArangoDBVertex.build(graph, simpleVertex);    			
    		}

    		public void remove() {
    			cursor.close();
    		}
    		
    	};
    	
    }

}
