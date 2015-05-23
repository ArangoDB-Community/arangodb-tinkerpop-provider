//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb;

import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBBaseQuery;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBPropertyFilter;

/**
 * The ArangoDB graph query class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBGraphQuery implements GraphQuery {

	/**
	 * the logger
	 */
	private static Logger LOG = Logger.getLogger(ArangoDBVertexIterable.class);

	private final ArangoDBGraph graph;
	private Long limit = null;
	private ArangoDBPropertyFilter propertyFilter = new ArangoDBPropertyFilter();
	private boolean count;

	/**
	 * Creates a graph query for a arangodb graph
	 * 
	 * @param graph
	 *            the arangodb graph
	 */
	public ArangoDBGraphQuery(final ArangoDBGraph graph) {
		this.graph = graph;
		this.count = false;
	}

	public <T extends Comparable<T>> GraphQuery has(final String key, final T value, final Compare compare) {
		switch (compare) {
		case EQUAL:
			propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.EQUAL);
			break;
		case NOT_EQUAL:
			propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.NOT_EQUAL);
			break;
		case GREATER_THAN:
			propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.GREATER_THAN);
			break;
		case LESS_THAN:
			propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.LESS_THAN);
			break;
		case GREATER_THAN_EQUAL:
			propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.GREATER_THAN_EQUAL);
			break;
		case LESS_THAN_EQUAL:
			propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.LESS_THAN_EQUAL);
			break;
		}
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

	public GraphQuery has(String key) {
		propertyFilter.has(key, null, ArangoDBPropertyFilter.Compare.HAS);
		return this;
	}

	public GraphQuery hasNot(String key) {
		propertyFilter.has(key, null, ArangoDBPropertyFilter.Compare.HAS_NOT);
		return this;
	}

	public GraphQuery has(String key, Object value) {
		propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.EQUAL);
		return this;
	}

	public GraphQuery hasNot(String key, Object value) {
		propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.NOT_EQUAL);
		return this;
	}

	public GraphQuery has(String key, Predicate prdct, Object value) {
		if (prdct instanceof com.tinkerpop.blueprints.Compare) {
			com.tinkerpop.blueprints.Compare compare = (com.tinkerpop.blueprints.Compare) prdct;

			switch (compare) {
			case EQUAL:
				propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.EQUAL);
				break;
			case NOT_EQUAL:
				propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.NOT_EQUAL);
				break;
			case GREATER_THAN:
				propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.GREATER_THAN);
				break;
			case LESS_THAN:
				propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.LESS_THAN);
				break;
			case GREATER_THAN_EQUAL:
				propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.GREATER_THAN_EQUAL);
				break;
			case LESS_THAN_EQUAL:
				propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.LESS_THAN_EQUAL);
				break;
			}
		} else if (prdct instanceof com.tinkerpop.blueprints.Contains) {

		}
		return this;
	}

	public <T extends Comparable<?>> GraphQuery interval(String key, T startValue, T endValue) {
		propertyFilter.has(key, startValue, ArangoDBPropertyFilter.Compare.GREATER_THAN_EQUAL);
		propertyFilter.has(key, endValue, ArangoDBPropertyFilter.Compare.LESS_THAN);
		return this;
	}

	public GraphQuery limit(int limit) {
		this.limit = new Long(limit);
		return this;
	}

}
