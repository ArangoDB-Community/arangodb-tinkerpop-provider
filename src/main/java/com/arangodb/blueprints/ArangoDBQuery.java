package com.arangodb.blueprints;

import com.arangodb.blueprints.client.ArangoDBPropertyFilter;
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
			com.tinkerpop.blueprints.Contains contains = (com.tinkerpop.blueprints.Contains) prdct;

			switch (contains) {
			case IN:
				propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.IN);
				break;
			case NOT_IN:
				propertyFilter.has(key, value, ArangoDBPropertyFilter.Compare.NOT_IN);
				break;
			}
		}
		return this;
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
