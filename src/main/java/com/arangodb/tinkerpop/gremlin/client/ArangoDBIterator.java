//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.util.Iterator;

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;

/**
 * The ArangoDBIterator is used to wrap ArangoDB documents from a query iterator into Graph elements: Vertex, Edge,
 * and Property.
 *
 * @param <IType> the Graph Element type returned at each iteration
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBIterator<IType> implements Iterator<IType> {
	
	private final ArangoCursor<? extends IType> delegate;
	
	private final ArangoDBGraph graph;
	
	/**
	 * Instantiates a new ArangoDB iterator.
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
		ArangoDBBaseDocument next = (ArangoDBBaseDocument) delegate.next();
		next.graph(graph);
		next.collection(graph.getPrefixedCollectioName(next.label));
		next.setPaired(true);
		return (IType) next;
	}
	
}