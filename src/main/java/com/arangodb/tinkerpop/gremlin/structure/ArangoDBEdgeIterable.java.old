//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBSimpleEdge;
import org.apache.log4j.Logger;

import com.arangodb.ArangoException;
import com.arangodb.CursorResult;
import com.tinkerpop.blueprints.Edge;

/**
 * The edge iterable
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * 
 */

public class ArangoDBEdgeIterable implements Iterable<Edge> {

	/**
	 * the logger
	 */
	private static final Logger logger = Logger.getLogger(ArangoDBEdgeIterable.class);

	private final ArangoDBGraph graph;
	private final ArangoDBQuery query;

	/**
	 * Creates the edge iterable by a graph and a query
	 * 
	 * @param graph
	 *            the arangodb graph
	 * @param query
	 *            the query
	 */
	public ArangoDBEdgeIterable(final ArangoDBGraph graph, final ArangoDBQuery query) {
		this.graph = graph;
		this.query = query;
	}

	@Override
	public Iterator<Edge> iterator() {
		return new EdgeIterator(query);
	}

	class EdgeIterator implements Iterator<Edge> {

		@SuppressWarnings("rawtypes")
		private CursorResult<Map> iter;

		public EdgeIterator(ArangoDBQuery query) {
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
		public Edge next() {
			if (iter == null || !iter.iterator().hasNext()) {
				throw new NoSuchElementException();
			}

			try {
				return ArangoDBEdge.build(graph, new ArangoDBSimpleEdge(iter.iterator().next()), null, null);
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

	};

}
