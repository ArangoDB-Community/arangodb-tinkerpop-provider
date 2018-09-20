//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Property;

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBElementProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

/**
 * The ArangoDBIterator is used to wrap Arango DB Element Properties from a query iterator into Graph
 * elements: Vertex, Edge, Property. A separate iterator is needed since Properties have a generic
 * type.
 *	
 * @see ArangoDBIterator
 * @param <V> 	the Property's type
 * @param <P> 	the Property implementation returned by the iterator
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBPropertyIterator<V, P extends Property<V>> implements Iterator<P> {
	
	/** The delegate. */
	
	private final ArangoCursor<? extends Property<V>> delegate;
	
	/** The graph. */
	
	private final ArangoDBGraph graph;
	
	/**
	 * Instantiates a new arango DB iterator.
	 *
	 * @param graph 				the graph
	 * @param documentNeighbors 	the delegate cursor
	 */
	public ArangoDBPropertyIterator(ArangoDBGraph graph, ArangoCursor<? extends Property<V>> documentNeighbors) {
		super();
		this.delegate = documentNeighbors;
		this.graph = graph;
	}

	@Override
	public boolean hasNext() {
		return delegate.hasNext();
	}

	@SuppressWarnings("unchecked")
	@Override
	public P next() {
		ArangoDBElementProperty<V> next = null;
		next = (ArangoDBElementProperty<V>) delegate.next();
		next.graph(graph);
		next.setPaired(true);
		return (P) next;
	}
	
}