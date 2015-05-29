//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints;

import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.arangodb.blueprints.client.ArangoDBBaseQuery;
import com.arangodb.blueprints.client.ArangoDBException;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;

/**
 * The ArangoDB graph query class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBGraphQuery extends ArangoDBQuery implements GraphQuery {

	/**
	 * the logger
	 */
	private static Logger LOG = Logger.getLogger(ArangoDBVertexIterable.class);

	/**
	 * Creates a graph query for a ArangoDB graph
	 * 
	 * @param graph
	 *            the ArangoDB graph
	 */
	public ArangoDBGraphQuery(final ArangoDBGraph graph) {
		super(graph);
	}

	public <T extends Comparable<T>> ArangoDBGraphQuery has(final String key, final T value, final Compare compare) {
		super.has(key, value, compare);
		return this;
	}

	public Iterable<Edge> edges() {
		ArangoDBBaseQuery query;
		try {
			query = graph.getClient().getGraphEdges(graph.getRawGraph(), propertyFilter, new Vector<String>(), limit,
				count);
			return new ArangoDBEdgeIterable(graph, query);
		} catch (ArangoDBException e) {
			return new ArangoDBEdgeIterable(graph, null);
		}
	}

	public Iterable<Vertex> vertices() {
		ArangoDBBaseQuery query;
		try {
			query = graph.getClient().getGraphVertices(graph.getRawGraph(), propertyFilter, limit, count);
			return new ArangoDBVertexIterable(graph, query);
		} catch (ArangoDBException e) {
			return new ArangoDBVertexIterable(graph, null);
		}
	}

	/**
	 * Executes the query and returns the number of result elements
	 * 
	 * @return number of elements
	 */
	public long count() {
		ArangoDBBaseQuery query;
		try {
			query = graph.getClient().getGraphEdges(graph.getRawGraph(), propertyFilter, new Vector<String>(), limit,
				true);

			return query.getCursorResult().getCount();
		} catch (ArangoDBException e) {
			LOG.error("error in AQL query", e);
		}

		return -1;
	}

	/**
	 * Executes the query and returns the identifiers of result elements
	 * 
	 * @return the identifiers of result elements
	 */
	public Iterator<String> vertexIds() {

		return new Iterator<String>() {

			private Iterator<Vertex> iter = vertices().iterator();

			public boolean hasNext() {
				return iter.hasNext();
			}

			public String next() {
				if (!iter.hasNext()) {
					return null;
				}

				Vertex v = iter.next();

				if (v == null) {
					return null;
				}

				return v.getId().toString();
			}

			public void remove() {
				iter.remove();
			}

		};
	}

	public ArangoDBGraphQuery has(String key) {
		super.has(key);
		return this;
	}

	public ArangoDBGraphQuery hasNot(String key) {
		super.hasNot(key);
		return this;
	}

	public ArangoDBGraphQuery has(String key, Object value) {
		super.has(key, value);
		return this;
	}

	public ArangoDBGraphQuery hasNot(String key, Object value) {
		super.hasNot(key, value);
		return this;
	}

	public ArangoDBGraphQuery has(String key, Predicate prdct, Object value) {
		super.has(key, prdct, value);
		return this;
	}

	public <T extends Comparable<?>> ArangoDBGraphQuery interval(String key, T startValue, T endValue) {
		super.interval(key, startValue, endValue);
		return this;
	}

	public ArangoDBGraphQuery limit(int limit) {
		super.limit(limit);
		return this;
	}

}
