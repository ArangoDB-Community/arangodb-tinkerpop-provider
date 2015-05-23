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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBBaseQuery;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBPropertyFilter;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleVertex;

/**
 * The ArangoDB vertex query class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBVertexQuery implements VertexQuery {

	/**
	 * the logger
	 */
	private static Logger LOG = Logger.getLogger(ArangoDBVertexIterable.class);

	private final ArangoDBGraph graph;
	private final ArangoDBSimpleVertex vertex;
	private ArangoDBBaseQuery.Direction direction = ArangoDBBaseQuery.Direction.ALL;
	private Vector<String> labels = null;
	private Long limit = null;
	private ArangoDBPropertyFilter propertyFilter = new ArangoDBPropertyFilter();
	private boolean count;

	/**
	 * Creates a arangodb vertex query for a graph and a vertex
	 * 
	 * @param graph
	 *            the arangodb graph
	 * @param vertex
	 *            the vertex
	 */
	public ArangoDBVertexQuery(final ArangoDBGraph graph, final ArangoDBVertex vertex) {
		this.graph = graph;
		this.vertex = vertex.getRawVertex();
		this.labels = new Vector<String>();
		this.count = false;
	}

	public VertexQuery has(String key, Object value) {
		propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.EQUAL);
		return this;
	}

	/**
	 * Filter out the element if it does not have a property with a comparable
	 * value.
	 *
	 * @param key
	 *            the key of the property
	 * @param value
	 *            the value to check against
	 * @param compare
	 *            the comparator to use for comparison
	 * @return the modified query object
	 * @deprecated
	 */
	@Deprecated
	public <T extends Comparable<T>> VertexQuery has(String key, T value, Compare compare) {
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

	public VertexQuery direction(final Direction direction) {
		if (direction == Direction.IN) {
			this.direction = ArangoDBBaseQuery.Direction.IN;
		} else if (direction == Direction.OUT) {
			this.direction = ArangoDBBaseQuery.Direction.OUT;
		} else {
			this.direction = ArangoDBBaseQuery.Direction.ALL;
		}
		return this;
	}

	public VertexQuery labels(final String... labels) {
		if (labels == null) {
			return this;
		}
		this.labels = new Vector<String>();

		for (String label : labels) {
			this.labels.add(label);
		}

		return this;
	}

	public Iterable<Edge> edges() {
		ArangoDBBaseQuery query;
		try {
			query = graph.getClient().getVertexEdges(graph.getRawGraph(), vertex, propertyFilter, labels, direction,
				limit, count);
			return new ArangoDBEdgeIterable(graph, query);
		} catch (ArangoDBException e) {
			return new ArangoDBEdgeIterable(graph, null);
		}
	}

	public Iterable<Vertex> vertices() {
		ArangoDBBaseQuery query;
		try {
			query = graph.getClient().getVertexNeighbors(graph.getRawGraph(), vertex, propertyFilter, labels,
				direction, limit, count);
			return new ArangoDBVertexIterable(graph, query);
		} catch (ArangoDBException e) {
			return new ArangoDBVertexIterable(graph, null);
		}
	}

	public long count() {
		ArangoDBBaseQuery query;
		try {
			query = graph.getClient().getVertexEdges(graph.getRawGraph(), vertex, propertyFilter, labels, direction,
				limit, true);

			return query.getCursorResult().getCount();
		} catch (ArangoDBException e) {
			LOG.error("error in AQL query", e);
		}

		return -1;
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

	public VertexQuery has(String key) {
		propertyFilter.has(key, null, ArangoDBPropertyFilter.Compare.HAS);
		return this;
	}

	public VertexQuery hasNot(String key) {
		propertyFilter.has(key, null, ArangoDBPropertyFilter.Compare.HAS_NOT);
		return this;
	}

	public VertexQuery hasNot(String key, Object value) {
		propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.NOT_EQUAL);
		return this;
	}

	public VertexQuery has(String key, Predicate prdct, Object value) {
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

	public <T extends Comparable<?>> VertexQuery interval(String key, T startValue, T endValue) {
		propertyFilter.has(key, startValue, ArangoDBPropertyFilter.Compare.GREATER_THAN_EQUAL);
		propertyFilter.has(key, endValue, ArangoDBPropertyFilter.Compare.LESS_THAN);
		return this;
	}

	public VertexQuery limit(int limit) {
		this.limit = new Long(limit);
		return this;
	}

}
