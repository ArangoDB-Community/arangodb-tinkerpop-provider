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

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBBaseQuery;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBPropertyFilter;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleEdgeCursor;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleEdgeQuery;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleVertexQuery;

/**
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBGraphQuery implements GraphQuery {

	private final ArangoDBGraph graph;
	private ArangoDBBaseQuery.Direction direction = ArangoDBBaseQuery.Direction.ALL;
	private Long limit = null;
	private ArangoDBPropertyFilter propertyFilter = new ArangoDBPropertyFilter();
	private boolean count;

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
		ArangoDBSimpleEdgeQuery query;
		try {
			query = graph.client.getGraphEdges(graph.getRawGraph(), propertyFilter, new Vector<String>(), limit, count);
			return new ArangoDBEdgeIterable(graph, query);
		} catch (ArangoDBException e) {
			return new ArangoDBEdgeIterable(graph, null);
		}
	}

	public Iterable<Vertex> vertices() {
		ArangoDBSimpleVertexQuery query;
		try {
			query = graph.client.getGraphVertices(graph.getRawGraph(), propertyFilter, limit, count);
			return new ArangoDBVertexIterable(graph, query);
		} catch (ArangoDBException e) {
			return new ArangoDBVertexIterable(graph, null);
		}
	}

	public long count() {
		this.count = true;
		long result = 0;

		ArangoDBSimpleEdgeQuery query;
		try {
			query = graph.client.getGraphEdges(graph.getRawGraph(), propertyFilter, new Vector<String>(), limit, count);

			ArangoDBSimpleEdgeCursor cursor = query.getResult();

			result = cursor.count();
		} catch (ArangoDBException e) {
		}

		this.count = false;
		return result;
	}

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
