//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.util.Iterator;

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

/**
 * The ArangoDBIterator is used to wrap Arango DB documents from a query iterator into Graph
 * elements: Vertex, Edge, Property.
 *
 * @param <IType> the Graph Element type returned at each iteration
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBIterator<IType> implements Iterator<IType> {
	
	/** The delegate. */
	
	private final ArangoCursor<? extends IType> delegate;
	
	/** The graph. */
	
	private final ArangoDBGraph graph;
	
	/**
	 * Instantiates a new arango DB iterator.
	 *
	 * @param graph the graph
	 * @param delegate the delegate
	 */
	public ArangoDBIterator(ArangoDBGraph graph, ArangoCursor<? extends IType> delegate) {
		super();
		this.delegate = delegate;
		this.graph = graph;
	}

	@Override
	public boolean hasNext() {
		return delegate.hasNext();
	}

	@SuppressWarnings("unchecked")
	@Override
	public IType next() {
		ArangoDBBaseDocument next = null;
		next = (ArangoDBBaseDocument) delegate.next();
		next.graph(graph);
		next.setPaired(true);
		return (IType) next;
	}
	
}