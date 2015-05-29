//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.arangodb.blueprints.client.ArangoDBBaseQuery;
import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.client.ArangoDBPropertyFilter;
import com.arangodb.blueprints.client.ArangoDBSimpleVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * The ArangoDB vertex query class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBVertexQuery extends ArangoDBQuery implements VertexQuery {

	/**
	 * the logger
	 */
	private static Logger LOG = Logger.getLogger(ArangoDBVertexIterable.class);

	private final ArangoDBSimpleVertex vertex;
	private ArangoDBBaseQuery.Direction direction;
	private List<String> labels = null;

	/**
	 * Creates a arangodb vertex query for a graph and a vertex
	 * 
	 * @param graph
	 *            the arangodb graph
	 * @param vertex
	 *            the vertex
	 */
	public ArangoDBVertexQuery(final ArangoDBGraph graph, final ArangoDBVertex vertex) {
		super(graph);
		this.vertex = vertex.getRawVertex();
		this.direction = ArangoDBBaseQuery.Direction.ALL;
		this.labels = new ArrayList<String>();
	}

	public ArangoDBVertexQuery has(String key, Object value) {
		super.has(key, value);
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
	public <T extends Comparable<T>> ArangoDBVertexQuery has(String key, T value, Compare compare) {
		super.has(key, value, compare);
		return this;
	}

	public ArangoDBVertexQuery direction(final Direction direction) {
		if (direction == Direction.IN) {
			this.direction = ArangoDBBaseQuery.Direction.IN;
		} else if (direction == Direction.OUT) {
			this.direction = ArangoDBBaseQuery.Direction.OUT;
		} else {
			this.direction = ArangoDBBaseQuery.Direction.ALL;
		}
		return this;
	}

	public ArangoDBVertexQuery labels(final String... labels) {
		if (labels == null) {
			return this;
		}
		this.labels = new ArrayList<String>();

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

	public ArangoDBVertexQuery has(String key) {
		super.has(key);
		return this;
	}

	public ArangoDBVertexQuery hasNot(String key) {
		super.hasNot(key);
		propertyFilter.has(key, null, ArangoDBPropertyFilter.Compare.HAS_NOT);
		return this;
	}

	public ArangoDBVertexQuery hasNot(String key, Object value) {
		super.hasNot(key, value);
		return this;
	}

	public ArangoDBVertexQuery has(String key, Predicate prdct, Object value) {
		super.has(key, prdct, value);
		return this;
	}

	public <T extends Comparable<?>> ArangoDBVertexQuery interval(String key, T startValue, T endValue) {
		super.interval(key, startValue, endValue);
		return this;
	}

	public ArangoDBVertexQuery limit(int limit) {
		super.limit(limit);
		return this;
	}

}
