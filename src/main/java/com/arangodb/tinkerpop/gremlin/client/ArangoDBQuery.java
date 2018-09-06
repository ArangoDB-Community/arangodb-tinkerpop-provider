//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

/**
 * The ArangoDB base query class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBQuery {
	
	/** The Logger. */
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBQuery.class);

	/** the ArangoDB graph. */
	
	protected ArangoDBGraph graph;

	/** the ArangoDB client. */
	
	protected ArangoDBGraphClient client;

	/** query type. */
	
	protected QueryType queryType;

	/** The start vertex. */
	
	protected ArangoDBBaseDocument startVertex;
	
	/** The property filter. */
	
	protected ArangoDBPropertyFilter propertyFilter;
	
	/** The labels filter. */
	
	protected List<String> labelsFilter = new ArrayList<>();
	
	/** The direction. */
	
	protected Direction direction;
	
	/** The limit. */
	
	protected Long limit;
	
	/** The count. */
	
	protected boolean count;
	
	/** The keys filter. */
	
	private List<String> keysFilter = new ArrayList<>();

	/**
	 * Direction.
	 */

	public enum Direction {
		
		/** direction into a element */
		IN,
		
		/** direction out of a element */
		OUT,
		
		/** direction out and in of a element */
		ALL
	};

	/**
	 * The Enum QueryType.
	 */
	
	public enum QueryType {
		
		/** The graph vertices. */
		GRAPH_VERTICES,
		
		/** The graph edges. */
		GRAPH_EDGES,
		
		/** The graph neighbors. */
		GRAPH_NEIGHBORS
	}

	/**
	 * Constructor.
	 *
	 * @param graph            the graph of the query
	 * @param client            the request client of the query
	 * @param queryType            a query type
	 */
	
	public ArangoDBQuery(ArangoDBGraph graph, ArangoDBGraphClient client, QueryType queryType) {
		this.graph = graph;
		this.client = client;
		this.queryType = queryType;
	}

	/**
	 * Constructs the AQL query string, executes the query and returns a result cursor.
	 *
	 * @param <T> 				the type returned by the cursor
	 * @param type            	The type of the result (POJO class, VPackSlice, String for Json, or Collection/List/Map)
	 * @return the cursor result
	 */
	
	@SuppressWarnings("rawtypes")
	public <T> ArangoCursor getCursorResult(final Class<T> type) {

		Map<String, Object> bindVars = new HashMap<>();
		String prefix = "";
		String returnExp;
		boolean joinFilter = false;
		StringBuilder sb = new StringBuilder();

		String same_collection = " FILTER IS_SAME_COLLECTION(";
		switch (queryType) {
		case GRAPH_VERTICES:
			if (graph.vertexCollections().size() > 1) {
				sb.append("FOR v in UNION( \n");	// Union of all vertex collections
				sb.append(graph.vertexCollections().stream()
					.map(vc -> ArangoDBUtil.getCollectioName(graph.name(), vc))
					.map(vc -> String.format("FOR v IN %s  RETURN v", vc))
					.collect(Collectors.joining("),\n  (", "  (", ")\n  ")));
				sb.append(")");
			}
			else {
				String collectionName = ArangoDBUtil.getCollectioName(graph.name(), graph.vertexCollections().get(0));
				sb.append(String.format("FOR v IN %s \n", collectionName));
			}
			if (!keysFilter.isEmpty()) {
				sb.append("FILTER v._key IN @keys ");
				bindVars.put("keys", keysFilter);
				joinFilter = true;
			}
			prefix = "v.";
			returnExp = "RETURN v";
			break;
		case GRAPH_EDGES:
			if (getStartVertex() == null) {
				if (graph.edgeCollections().size() > 1) {
					sb.append("FOR e in UNION( \n");	// Union of all vertex collections
					sb.append(graph.edgeCollections().stream()
							.map(ec -> ArangoDBUtil.getCollectioName(graph.name(), ec))
							.map(ec -> String.format("FOR e IN %s RETURN e", ec))
							.collect(Collectors.joining("),\n  (", "  (", ")\n  ")));
					sb.append(")\n");
				}
				else {
					String collectionName = ArangoDBUtil.getCollectioName(graph.name(), graph.edgeCollections().get(0));
					sb.append(String.format("FOR e IN %s \n", collectionName));
				}
				if (!keysFilter.isEmpty()) {
					sb.append("FILTER e._key IN @keys ");
					bindVars.put("keys", keysFilter);
					joinFilter = true;
				}
				prefix = "e.";
				returnExp = "RETURN e";
			}
			else {
				sb.append(String.format("FOR v, e IN %s @startId GRAPH @graphName \n", getDirectionString()));
				sb.append("  OPTIONS {bfs: true}\n");
				if (!keysFilter.isEmpty()) {
					sb.append("FILTER e._id IN @keys ");
					bindVars.put("keys", keysFilter);
					joinFilter = true;
				}
				if (!labelsFilter.isEmpty()) {
					if (joinFilter) {
						same_collection = " AND " + same_collection;
					}
					String filter = labelsFilter.stream()
                            .map(lbl -> ArangoDBUtil.getCollectioName(graph.name(), lbl))
                            .collect(Collectors.joining(", e) OR IS_SAME_COLLECTION(", same_collection, ", e) "));
					sb.append(filter);
					joinFilter = true;
				}
				prefix = "e.";
				returnExp = "RETURN DISTINCT e";
				bindVars.put("startId", startVertex._id());
				bindVars.put("graphName", graph.name());
			}
			break;
		case GRAPH_NEIGHBORS:
		default:
			// The labelsFilter is used to filter the collection of the edges
			sb.append(String.format("FOR v, e IN 1 %s @startId GRAPH @graphName\n", getDirectionString()));
			sb.append("  OPTIONS {bfs: true, uniqueVertices: 'global'}\n");
			if (!labelsFilter.isEmpty()) {
                String filter = labelsFilter.stream()
                        .map(lbl -> ArangoDBUtil.getCollectioName(graph.name(), lbl))
                        .collect(Collectors.joining(", e) OR IS_SAME_COLLECTION(", same_collection, ", e) "));
				sb.append(filter);
                joinFilter = true;
			}
			prefix = "v.";
			returnExp = "  RETURN v";
			bindVars.put("startId", startVertex._id());
			bindVars.put("graphName", graph.name());
			break;
		}

		List<String> andFilter = new ArrayList<String>();

		if (propertyFilter == null) {
			propertyFilter = new ArangoDBPropertyFilter();
		}
		propertyFilter.addAqlSegments(prefix, andFilter, bindVars);

		if (CollectionUtils.isNotEmpty(andFilter)) {
			if (joinFilter) {
				sb.append(" AND ");
			} else {
                sb.append(" FILTER ");
            }
			sb.append(StringUtils.join(andFilter, " AND "));
		}

		if (limit != null) {
			sb.append(" LIMIT " + limit.toString());
		}

		sb.append(returnExp);

		String query = sb.toString();
		logger.debug("ArangoDB query: {}", query);
		logger.debug("Binded Vars: {}", bindVars);
		AqlQueryOptions aqlQueryOptions = new AqlQueryOptions();
		aqlQueryOptions.count(count);

		return client.executeAqlQuery(query, bindVars, aqlQueryOptions, type);
	}


	/**
	 * Gets the direction string.
	 *
	 * @return the direction string
	 */
	
	private String getDirectionString() {
		if (direction != null) {
			if (direction == Direction.IN) {
				return "INBOUND";
			} else if (direction == Direction.OUT) {
				return "OUTBOUND";
			}
		}
		return "ANY";
	}

	/**
	 * Gets the start vertex.
	 *
	 * @return the start vertex
	 */
	
	public ArangoDBBaseDocument getStartVertex() {
		return startVertex;
	}

	/**
	 * Sets the start vertex.
	 *
	 * @param startVertex the start vertex
	 * @return the arango DB query
	 */
	
	public ArangoDBQuery setStartVertex(ArangoDBBaseDocument startVertex) {
		this.startVertex = startVertex;
		return this;
	}

	/**
	 * Gets the property filter.
	 *
	 * @return the property filter
	 */
	
	public ArangoDBPropertyFilter getPropertyFilter() {
		return propertyFilter;
	}

	/**
	 * Sets the property filter.
	 *
	 * @param propertyFilter the property filter
	 * @return the arango DB query
	 */
	
	public ArangoDBQuery setPropertyFilter(ArangoDBPropertyFilter propertyFilter) {
		this.propertyFilter = propertyFilter;
		return this;
	}

	/**
	 * Gets the labels filter.
	 *
	 * @return the labels filter
	 */
	
	public List<String> getLabelsFilter() {
		return labelsFilter;
	}

	/**
	 * Sets the labels filter.
	 *
	 * @param labelsFilter the labels filter
	 * @return the arango DB query
	 */
	
	public ArangoDBQuery setLabelsFilter(List<String> labelsFilter) {
		this.labelsFilter = labelsFilter;
		return this;
	}

	/**
	 * Gets the direction.
	 *
	 * @return the direction
	 */
	
	public Direction getDirection() {
		return direction;
	}

	/**
	 * Sets the direction.
	 *
	 * @param direction the direction
	 * @return the arango DB query
	 */
	
	public ArangoDBQuery setDirection(Direction direction) {
		this.direction = direction;
		return this;
	}

	/**
	 * Gets the limit.
	 *
	 * @return the limit
	 */
	
	public Long getLimit() {
		return limit;
	}

	/**
	 * Sets the limit.
	 *
	 * @param limit the limit
	 * @return the arango DB query
	 */
	
	public ArangoDBQuery setLimit(Long limit) {
		this.limit = limit;
		return this;
	}

	/**
	 * Checks if is count.
	 *
	 * @return true, if is count
	 */
	public boolean isCount() {
		return count;
	}

	/**
	 * Sets the count.
	 *
	 * @param count the count
	 * @return the arango DB query
	 */
	
	public ArangoDBQuery setCount(boolean count) {
		this.count = count;
		return this;
	}

	/**
	 * Sets the keys filter.
	 *
	 * @param ids the ids
	 * @return the arango DB query
	 */
	
	public ArangoDBQuery setKeysFilter(List<String> ids) {
		this.keysFilter = ids;
		return this;
	}

}
