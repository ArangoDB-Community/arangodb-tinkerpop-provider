//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * The VariableIterator is used to wrap and ArangoDB iterator and return ArangoDBGraphVariables that use a specific
 * GraphVariablesClient during.
 *
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class VertexIterator implements Iterator<Vertex> {

	private final ArangoCursor<ArangoDBVertex> delegate;

	private final VertexClient client;

	/**
	 * Instantiates a new ArangoDB iterator.
	 *
	 * @param client 				the graph client
	 * @param delegate 				the delegate iterator
	 */
	public VertexIterator(VertexClient client, ArangoCursor<ArangoDBVertex> delegate) {
		super();
		this.delegate = delegate;
		this.client = client;
	}

	@Override
	public boolean hasNext() {
		return delegate.hasNext();
	}

	@Override
	public ArangoDBVertex next() {
		return delegate.next().useClient(client);
	}

}