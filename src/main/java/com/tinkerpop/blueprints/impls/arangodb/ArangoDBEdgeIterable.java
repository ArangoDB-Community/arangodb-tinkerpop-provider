//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.arangodb.ArangoException;
import com.arangodb.CursorResult;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBBaseQuery;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleEdge;

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
	private static Logger LOG = Logger.getLogger(ArangoDBEdgeIterable.class);

	private final ArangoDBGraph graph;
	private final ArangoDBBaseQuery query;

	/**
	 * Creates the edge iterable by a graph and a query
	 * 
	 * @param graph
	 *            the arangodb graph
	 * @param query
	 *            the query
	 */
	public ArangoDBEdgeIterable(final ArangoDBGraph graph, final ArangoDBBaseQuery query) {
		this.graph = graph;
		this.query = query;
	}

	public Iterator<Edge> iterator() {

		return new Iterator<Edge>() {

			@SuppressWarnings("rawtypes")
			private CursorResult<Map> iter;

			{
				try {
					if (query != null) {
						iter = query.getCursorResult();
					}
				} catch (ArangoDBException e) {
					LOG.error("error in AQL request", e);
				}
			}

			public boolean hasNext() {
				if (iter == null) {
					return false;
				}
				return iter.iterator().hasNext();
			}

			@SuppressWarnings("unchecked")
			public Edge next() {
				if (iter == null || !iter.iterator().hasNext()) {
					throw new NoSuchElementException();
				}

				try {
					return ArangoDBEdge.build(graph, new ArangoDBSimpleEdge(iter.iterator().next()), null, null);
				} catch (ArangoDBException e) {
					LOG.error("iterator.next", e);
					return null;
				}
			}

			public void remove() {
				if (iter != null) {
					try {
						iter.close();
					} catch (ArangoException e) {
						LOG.error("could not close iterator", e);
					}
				}
			}

		};

	}

}
