//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.arangodb.ArangoException;
import com.arangodb.CursorResult;
import com.arangodb.blueprints.client.ArangoDBBaseQuery;
import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.client.ArangoDBSimpleVertex;
import com.tinkerpop.blueprints.Vertex;

/**
 * The ArangoDB vertex iterable class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * 
 */
public class ArangoDBVertexIterable implements Iterable<Vertex> {

	/**
	 * the logger
	 */
	private static final Logger logger = Logger.getLogger(ArangoDBVertexIterable.class);

	private final ArangoDBGraph graph;
	private final ArangoDBBaseQuery query;

	/**
	 * Creates a vertex iterable for a graph and a query
	 * 
	 * @param graph
	 *            the arangodb graph
	 * @param query
	 *            the query
	 */
	public ArangoDBVertexIterable(final ArangoDBGraph graph, final ArangoDBBaseQuery query) {
		this.graph = graph;
		this.query = query;
	}

	@Override
	public Iterator<Vertex> iterator() {
		return new VertexIterator(query);
	}

	class VertexIterator implements Iterator<Vertex> {

		@SuppressWarnings("rawtypes")
		private CursorResult<Map> iter;

		public VertexIterator(ArangoDBBaseQuery query) {
			try {
				if (query != null) {
					iter = query.getCursorResult();
				}
			} catch (ArangoDBException e) {
				logger.error("error in AQL request", e);
			}
		}

		@Override
		public boolean hasNext() {
			if (iter == null) {
				return false;
			}
			return iter.iterator().hasNext();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Vertex next() {
			if (iter == null || !iter.iterator().hasNext()) {
				throw new NoSuchElementException();
			}

			try {
				return ArangoDBVertex.build(graph, new ArangoDBSimpleVertex(iter.iterator().next()));
			} catch (ArangoDBException e) {
				logger.error("iterator.next", e);
				return null;
			}
		}

		@Override
		public void remove() {
			if (iter != null) {
				try {
					iter.close();
				} catch (ArangoException e) {
					logger.error("could not close iterator", e);
				}
			}
		}

	}
}
