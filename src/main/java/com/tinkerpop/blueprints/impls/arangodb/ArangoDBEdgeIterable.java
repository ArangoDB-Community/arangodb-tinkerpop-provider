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

import com.tinkerpop.blueprints.Edge;

/**
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 */

public class ArangoDBEdgeIterable<T extends Edge> implements Iterable<ArangoDBEdge> {
	
    private final ArangoDBGraph graph;
    private final ArangoDBSimpleEdgeQuery query;

    public ArangoDBEdgeIterable(final ArangoDBGraph graph, final ArangoDBSimpleEdgeQuery query) {
        this.graph = graph;
        this.query = query;
    }

    public Iterator<ArangoDBEdge> iterator() {
    	
    	return new Iterator<ArangoDBEdge>() {

    		private ArangoDBSimpleEdgeCursor cursor;

    		{
    			try {
    				// save changed edges and vertices
    				graph.save();
    				
    				if (query == null) {
    					// create a dummy cursor
        				cursor = new ArangoDBSimpleEdgeCursor(graph.client, null);    					
    				}
    				else {
        				cursor = query.getResult();    					
    				}
    			} catch (ArangoDBException e) {
    				cursor = new ArangoDBSimpleEdgeCursor(graph.client, null);
    			}    
    		}    		

    		public boolean hasNext() {
    			return cursor.hasNext();
    		}

    		public ArangoDBEdge next() {
    			if (!cursor.hasNext()) {
    				throw new NoSuchElementException();
    			}
    			
    			ArangoDBSimpleEdge simpleEdge = cursor.next();

    			if (simpleEdge == null) {
    				return null;
    			}
    			
    			return ArangoDBEdge.build(graph, simpleEdge, null, null);
    		}

    		public void remove() {
    			cursor.close();
    		}
    		
    	};
    	
    }

}
