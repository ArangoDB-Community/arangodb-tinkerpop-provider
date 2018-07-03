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
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.arangodb.ArangoCursor;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;


/**
 * The ArangoDB base query class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
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

	protected ArangoDBVertex<?> startVertex;
	protected ArangoDBPropertyFilter propertyFilter;
	protected List<String> labelsFilter = new ArrayList<>();
	protected Direction direction;
	protected Long limit;
	protected boolean count;
	private List<String> idsFilter = new ArrayList<>();

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
	 */
	public ArangoDBQuery(ArangoDBGraph graph, ArangoDBSimpleGraphClient client, QueryType queryType) {
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
	 * @throws ArangoDBGraphException             if the query could not be executed
	 */
	@SuppressWarnings("rawtypes")
	public <T> ArangoCursor getCursorResult(final Class<T> type) throws ArangoDBGraphException {

//		Map<String, Object> options = new HashMap<String, Object>();
//		options.put("includeData", true);
//		options.put("direction", getDirectionString());

		Map<String, Object> bindVars = new HashMap<String, Object>();
//		bindVars.put("graphName", graph.name());
//		bindVars.put("options", options);
//		bindVars.put("vertexExample", getVertexExample());

		StringBuilder sb = new StringBuilder();
		
		switch (queryType) {
		case GRAPH_VERTICES:
			if (idsFilter.isEmpty()) {
				if (graph.vertexCollections().size() > 1) {
					sb.append("FOR v in UNION( \n");	// Union of all vertex collections
					sb.append(graph.vertexCollections().stream()
						.map(vc -> ArangoDBUtil.getCollectioName(graph.name(), vc))
						.map(vc -> String.format("FOR v IN %s  RETURN v", vc))
						.collect(Collectors.joining(", ", "(", ")\n")));
					sb.append(")");
					sb.append("RETURN v");
				}
				else {
					String collectionName = ArangoDBUtil.getCollectioName(graph.name(), graph.vertexCollections().get(0));
					sb.append(String.format("FOR v IN %s \n", collectionName));
					sb.append("RETURN v");
				}
			}
			else {
				String with = graph.vertexCollections().stream()
						.map(vc -> ArangoDBUtil.getCollectioName(graph.name(), vc))
						.collect(Collectors.joining(","));
				sb.append(String.format("WITH %s ", with));
				sb.append("FOR n in DOCUMENT(@idString)");
				sb.append("  RETURN n");
				bindVars.put("idString", idsFilter);
			}
			break;
		case GRAPH_EDGES:
			if (getStartVertex() == null) {
				if (idsFilter.isEmpty()) {
					if (graph.edgeCollections().size() > 1) {
						sb.append("FOR e in UNION( \n");	// Union of all vertex collections
						sb.append(graph.edgeCollections().stream()
								.map(ec -> ArangoDBUtil.getCollectioName(graph.name(), ec))
								.map(ec -> String.format("FOR e IN %s  RETURN e", ec))
								.collect(Collectors.joining(", ", "(", ")\n")));
						sb.append(")\n");
						sb.append("RETURN e");
					}
					else {
						String collectionName = ArangoDBUtil.getCollectioName(graph.name(), graph.edgeCollections().get(0));
						sb.append(String.format("FOR e IN %s \n", collectionName));
						sb.append("RETURN e");
					}
				}
				else {
					String with = graph.edgeCollections().stream()
							.map(vc -> ArangoDBUtil.getCollectioName(graph.name(), vc))
							.collect(Collectors.joining(","));
					sb.append(String.format("WITH %s ", with));
					sb.append("FOR n in DOCUMENT(@idString)");
					sb.append("  RETURN n");
					bindVars.put("idString", idsFilter);
				}
			}
			else {
				sb.append(String.format("FOR v, e IN %s @startId GRAPH @graphName \n", getDirectionString()));
				if (!idsFilter.isEmpty()) {
					sb.append("FILTER e._id IN @edgeIds");
					bindVars.put("edgeIds", idsFilter);
				}
				sb.append("RETURN DISTINCT e");;
				bindVars.put("startId", startVertex._id());
				bindVars.put("graphName", graph.name());
			}
			break;
		case GRAPH_NEIGHBORS:
		default:
			if (labelsFilter.isEmpty()) {
				sb.append(String.format("FOR v IN %s @startId GRAPH @graphName OPTIONS {bfs: true, uniqueVertices: 'global'} RETURN v", getDirectionString()));	
			}
			else {
				sb.append(String.format("FOR v, e IN %s GRAPH @graphName OPTIONS {bfs: true} FILTER e.label IN @edgeLabels RETURN DISTINCT v", getDirectionString()));
				bindVars.put("@edgeLabels", labelsFilter);
			}
			// FIXME Can we filter on vertex label?
			bindVars.put("startId", startVertex._id());
			bindVars.put("graphName", graph.name());
			break;
		}

		List<String> andFilter = new ArrayList<String>();

		//if (propertyFilter == null) {
		//	propertyFilter = new ArangoDBPropertyFilter();
		//}
		//propertyFilter.addProperties(prefix, andFilter, bindVars);

//		if (CollectionUtils.isNotEmpty(labelsFilter)) {
//			List<String> orFilter = new ArrayList<String>();
//			int tmpCount = 0;
//			for (String label : labelsFilter) {
//				orFilter.add(prefix + "label == @label" + tmpCount);
//				bindVars.put("label" + tmpCount++, label);
//			}
//			if (CollectionUtils.isNotEmpty(orFilter)) {
//				andFilter.add("(" + StringUtils.join(orFilter, " OR ") + ")");
//			}
//		}

		if (CollectionUtils.isNotEmpty(andFilter)) {
			sb.append(" FILTER ");
			sb.append(StringUtils.join(andFilter, " AND "));
		}

		if (limit != null) {
			sb.append(" LIMIT " + limit.toString());
		}

//		sb.append(returnExp);

		String query = sb.toString();
		AqlQueryOptions aqlQueryOptions = new AqlQueryOptions();
		aqlQueryOptions.count(count);

		return client.executeAqlQuery(query, bindVars, aqlQueryOptions, type);
	}


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

	public ArangoDBVertex<?> getStartVertex() {
		return startVertex;
	}

	public ArangoDBQuery setStartVertex(ArangoDBVertex<?> startVertex) {
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
		this.labelsFilter = labelsFilter;	//.stream().map(lbl -> String.format("\"%s\"", lbl)).collect(Collectors.toList());;
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

	public ArangoDBQuery setVertexIds(List<String> ids) {
		this.idsFilter = ids; //.stream().map(id -> String.format("\"%s\"", id)).collect(Collectors.toList());
		return this;
	}

}
