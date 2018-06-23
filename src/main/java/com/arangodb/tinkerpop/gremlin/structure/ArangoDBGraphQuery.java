//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.Iterator;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBException;


/**
 * The ArangoDB graph query class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */
@Deprecated
public class ArangoDBGraphQuery extends ArangoDBQuery implements GraphQuery {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertexIterable.class);

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

	@Override
	public Iterable<Edge> edges() {
		ArangoDBQuery query;
		try {
			query = graph.getClient().getGraphEdges(graph.getRawGraph(), propertyFilter, new Vector<String>(), limit,
				count);
			return new ArangoDBEdgeIterable(graph, query);
		} catch (ArangoDBException e) {
			logger.debug("error while reading edges", e);
			return new ArangoDBEdgeIterable(graph, null);
		}
	}

	@Override
	public Iterable<Vertex> vertices() {
		ArangoDBQuery query;
		try {
			query = graph.getClient().getGraphVertices(graph.getRawGraph(), propertyFilter, limit, count);
			return new ArangoDBVertexIterable(graph, query);
		} catch (ArangoDBException e) {
			logger.debug("error while reading vertices", e);
			return new ArangoDBVertexIterable(graph, null);
		}
	}

	/**
	 * Executes the query and returns the number of result elements
	 * 
	 * @return number of elements
	 */
	public long count() {
		ArangoDBQuery query;
		try {
			query = graph.getClient().getGraphEdges(graph.getRawGraph(), propertyFilter, new Vector<String>(), limit,
				true);

			return query.getCursorResult().getCount();
		} catch (ArangoDBException e) {
			logger.error("error in AQL query", e);
		}

		return -1;
	}

	/**
	 * Executes the query and returns the identifiers of result elements
	 * 
	 * @return the identifiers of result elements
	 */
	public Iterator<String> vertexIds() {
		return new VertexIterator(vertices());
	}

	@Override
	public ArangoDBGraphQuery has(String key) {
		super.has(key);
		return this;
	}

	@Override
	public ArangoDBGraphQuery hasNot(String key) {
		super.hasNot(key);
		return this;
	}

	@Override
	public ArangoDBGraphQuery has(String key, Object value) {
		super.has(key, value);
		return this;
	}

	@Override
	public ArangoDBGraphQuery hasNot(String key, Object value) {
		super.hasNot(key, value);
		return this;
	}

	@Override
	public ArangoDBGraphQuery has(String key, Predicate prdct, Object value) {
		super.has(key, prdct, value);
		return this;
	}

	@Override
	public <T extends Comparable<?>> ArangoDBGraphQuery interval(String key, T startValue, T endValue) {
		super.interval(key, startValue, endValue);
		return this;
	}

	@Override
	public ArangoDBGraphQuery limit(int limit) {
		super.limit(limit);
		return this;
	}

	class VertexIterator implements Iterator<String> {

		private Iterator<Vertex> iter = vertices().iterator();

		public VertexIterator(Iterable<Vertex> iterable) {
			iter = iterable.iterator();
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
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

		@Override
		public void remove() {
			iter.remove();
		}

	}
}
