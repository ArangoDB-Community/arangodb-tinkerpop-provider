package com.arangodb.blueprints;

import com.arangodb.blueprints.client.ArangoDBPropertyFilter;
import com.tinkerpop.blueprints.Contains;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Query.Compare;

public class ArangoDBQuery {

	protected final ArangoDBGraph graph;
	protected Long limit = null;
	protected ArangoDBPropertyFilter propertyFilter = new ArangoDBPropertyFilter();
	protected boolean count;

	/**
	 * Creates a graph query for a ArangoDB graph
	 * 
	 * @param graph
	 *            the ArangoDB graph
	 */
	public ArangoDBQuery(final ArangoDBGraph graph) {
		this.graph = graph;
		this.count = false;
	}

	public <T extends Comparable<T>> ArangoDBQuery has(final String key, final T value, final Compare compare) {
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
		default:
			// do nothing
		}
		return this;
	}

	public ArangoDBQuery has(String key) {
		propertyFilter.has(key, null, ArangoDBPropertyFilter.Compare.HAS);
		return this;
	}

	public ArangoDBQuery hasNot(String key) {
		propertyFilter.has(key, null, ArangoDBPropertyFilter.Compare.HAS_NOT);
		return this;
	}

	public ArangoDBQuery has(String key, Object value) {
		propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.EQUAL);
		return this;
	}

	public ArangoDBQuery hasNot(String key, Object value) {
		propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.NOT_EQUAL);
		return this;
	}

	public ArangoDBQuery has(String key, Predicate prdct, Object value) {
		if (prdct instanceof com.tinkerpop.blueprints.Compare) {
			com.tinkerpop.blueprints.Compare compare = (com.tinkerpop.blueprints.Compare) prdct;
			hasCompare(key, value, compare);
		} else if (prdct instanceof com.tinkerpop.blueprints.Contains) {
			com.tinkerpop.blueprints.Contains contains = (com.tinkerpop.blueprints.Contains) prdct;
			hasContains(key, contains, value);
		}
		return this;
	}

	private void hasContains(String key, com.tinkerpop.blueprints.Contains contains, Object value) {
		if (contains == Contains.IN) {
			propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.IN);
		} else if (contains == Contains.NOT_IN) {
			propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.NOT_IN);
		}
	}

	private void hasCompare(String key, Object value, com.tinkerpop.blueprints.Compare compare) {
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
		default:
			// do nothing
		}
	}

	public <T extends Comparable<?>> ArangoDBQuery interval(String key, T startValue, T endValue) {
		propertyFilter.has(key, startValue, ArangoDBPropertyFilter.Compare.GREATER_THAN_EQUAL);
		propertyFilter.has(key, endValue, ArangoDBPropertyFilter.Compare.LESS_THAN);
		return this;
	}

	public ArangoDBQuery limit(int limit) {
		this.limit = new Long(limit);
		return this;
	}

}
