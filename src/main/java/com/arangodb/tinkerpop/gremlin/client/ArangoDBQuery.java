//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;


/**
 * The ArangoDB base query class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

public class ArangoDBQuery {

	/**
	 * the ArangoDB graph
	 */
	protected ArangoDBGraph graph;

	/**
	 * the ArangoDB client
	 */
	protected ArangoDBSimpleGraphClient client;

	/**
	 * query type
	 */
	protected QueryType queryType;

	protected ArangoDBVertex startVertex;
	protected ArangoDBPropertyFilter propertyFilter;
	protected List<String> labelsFilter;
	protected Direction direction;
	protected Long limit;
	protected boolean count;

	/**
	 * Direction
	 * 
	 */

	public enum Direction {
		// direction into a element
		IN,
		// direction out of a element
		OUT,
		// direction in and out of a element
		ALL
	};

	public enum QueryType {
		GRAPH_VERTICES,
		GRAPH_EDGES,
		GRAPH_NEIGHBORS
	}

	/**
	 * Constructor
	 * 
	 * @param graph
	 *            the graph of the query
	 * @param client
	 *            the request client of the query
	 * @param queryType
	 *            a query type
	 * @throws ArangoDBException
	 */
	public ArangoDBQuery(ArangoDBGraph graph, ArangoDBSimpleGraphClient client, QueryType queryType)
			throws ArangoDBException {
		this.graph = graph;
		this.client = client;
		this.queryType = queryType;
	}

	/**
	 * Executes the query and returns a result cursor
	 * @param type
	 *            The type of the result (POJO class, VPackSlice, String for Json, or Collection/List/Map)
	 * 
	 * @return CursorResult<Map> the result cursor
	 * 
	 * @throws ArangoDBException
	 *             if the query could not be executed
	 */
	@SuppressWarnings("rawtypes")
	public <T> ArangoCursor getCursorResult(final Class<T> type) throws ArangoDBException {

		Map<String, Object> options = new HashMap<String, Object>();
		options.put("includeData", true);
		options.put("direction", getDirectionString());

		Map<String, Object> bindVars = new HashMap<String, Object>();
		bindVars.put("graphName", graph.name());
		bindVars.put("options", options);
		bindVars.put("vertexExample", getVertexExample());

		StringBuilder sb = new StringBuilder();
		String prefix = "i.";
		String returnExp = " return i";

		switch (queryType) {
		case GRAPH_VERTICES:
			sb.append("for i in GRAPH_VERTICES(@graphName , @vertexExample, @options)");
			break;
		case GRAPH_EDGES:
			sb.append("for i in GRAPH_EDGES(@graphName , @vertexExample, @options)");
			break;
		case GRAPH_NEIGHBORS:
		default:
			sb.append("for i in GRAPH_EDGES(@graphName , @vertexExample, @options)");
			returnExp = " return DOCUMENT(" + getDocumentByDirection() + ")";
			break;
		}

		List<String> andFilter = new ArrayList<String>();

		if (propertyFilter == null) {
			propertyFilter = new ArangoDBPropertyFilter();
		}
		propertyFilter.addProperties(prefix, andFilter, bindVars);

		if (CollectionUtils.isNotEmpty(labelsFilter)) {
			List<String> orFilter = new ArrayList<String>();
			int tmpCount = 0;
			for (String label : labelsFilter) {
				orFilter.add(prefix + "label == @label" + tmpCount);
				bindVars.put("label" + tmpCount++, label);
			}
			if (CollectionUtils.isNotEmpty(orFilter)) {
				andFilter.add("(" + StringUtils.join(orFilter, " OR ") + ")");
			}
		}

		if (CollectionUtils.isNotEmpty(andFilter)) {
			sb.append(" FILTER ");
			sb.append(StringUtils.join(andFilter, " AND "));
		}

		if (limit != null) {
			sb.append(" LIMIT " + limit.toString());
		}

		sb.append(returnExp);

		String query = sb.toString();
		AqlQueryOptions aqlQueryOptions = new AqlQueryOptions();
		aqlQueryOptions.batchSize(client.batchSize());
		aqlQueryOptions.count(count);

		return client.executeAqlQuery(query, bindVars, aqlQueryOptions, type);
	}

	private Object getVertexExample() {
		if (startVertex != null) {
			return startVertex._id();
		} else {
			return new HashMap<String, String>();
		}
	}

	private String getDirectionString() {
		if (direction != null) {
			if (direction == Direction.IN) {
				return "inbound";
			} else if (direction == Direction.OUT) {
				return "outbound";
			}
		}

		return "any";
	}

	private String getDocumentByDirection() {
		if (direction != null) {
			if (direction == Direction.IN) {
				return "i._from";
			} else if (direction == Direction.OUT) {
				return "i._to";
			}
		}

		return "i._to == @vertexExample ? i._from : i._to";
	}

	public ArangoDBVertex getStartVertex() {
		return startVertex;
	}

	public ArangoDBQuery setStartVertex(ArangoDBVertex startVertex) {
		this.startVertex = startVertex;
		return this;
	}

	public ArangoDBPropertyFilter getPropertyFilter() {
		return propertyFilter;
	}

	public ArangoDBQuery setPropertyFilter(ArangoDBPropertyFilter propertyFilter) {
		this.propertyFilter = propertyFilter;
		return this;
	}

	public List<String> getLabelsFilter() {
		return labelsFilter;
	}

	public ArangoDBQuery setLabelsFilter(List<String> labelsFilter) {
		this.labelsFilter = labelsFilter;
		return this;
	}

	public Direction getDirection() {
		return direction;
	}

	public ArangoDBQuery setDirection(Direction direction) {
		this.direction = direction;
		return this;
	}

	public Long getLimit() {
		return limit;
	}

	public ArangoDBQuery setLimit(Long limit) {
		this.limit = limit;
		return this;
	}

	public boolean isCount() {
		return count;
	}

	public ArangoDBQuery setCount(boolean count) {
		this.count = count;
		return this;
	}

}
