//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

/**
 * The ArangoDB Query Util class provides static methods for building AQL fragments that can be
 * concatenated to build complete AQL queries.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */
public class ArangoDBQueryBuilder {
	
	/** The Logger. */
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBQueryBuilder.class);

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
	
	
	/** The query builder. */
	
	private StringBuilder queryBuilder;
	
	/** The iterate counter. */
	
	private int iterateCnt = 1;

	/** The filtered flag. */
	
	private boolean filtered = false;

	/** Whether the builder should prefix collection names with grpah name. */
	private Boolean shouldPrefixWithGraphName;

	/**
	 * Direction to navigate for vertex paths.
	 */

	public enum Direction {
		
		/**  direction into a element. */
		IN("INBOUND"),
		
		/**  direction out of a element. */
		OUT("OUTBOUND"),
		
		/**  direction out and in of a element. */
		ALL("ANY");
		
		/** The aql name. */
		private final String aqlName;
		
		/**
		 * Instantiates a new direction.
		 *
		 * @param aqlName the aql name
		 */
		Direction(String aqlName) {
			this.aqlName = aqlName;
		}
		
		/**
		 * Gets the aql name.
		 *
		 * @return the aql name
		 */
		String getAqlName() {
			return aqlName;
		}
		
	};
	
	/**
	 * Options for vertices in Graph Traversals.
	 *
	 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
	 */
	
	enum UniqueVertices {
		
		/** It is guaranteed that there is no path returned with a duplicate vertex. */
		PATH("path"),
		
		/** it is guaranteed that each vertex is visited at most once during the traversal, no
		 * matter how many paths lead from the start vertex to this one. */
		GLOBAL("global"),
		
		/** No uniqueness check is applied on vertices - (default). */
		NONE("none");
		
		/** The aql name. */
		private final String aqlName;
		
		/**
		 * Instantiates a new unique vertices.
		 *
		 * @param aqlName the aql name
		 */
		UniqueVertices(String aqlName) {
			this.aqlName = aqlName;
		}
		
		/**
		 * Gets the aql name.
		 *
		 * @return the aql name
		 */
		String getAqlName() {
			return aqlName;
		}
	}
	
	/**
	 * Options for edges in Graph Traversals.
	 *
	 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
	 */
	
	enum UniqueEdges {
		
		/** It is guaranteed that there is no path returned with a duplicate edge - (default). */
		PATH("path"),
		
		/** No uniqueness check is applied on edges. <b>Note</b>: Using this configuration the
		 * traversal will follow cycles in edges. */
		NONE("none");
		
		/** The aql name. */
		private final String aqlName;
		
		/**
		 * Instantiates a new unique edges.
		 *
		 * @param aqlName the aql name
		 */
		UniqueEdges(String aqlName) {
			this.aqlName = aqlName;
		}
		
		/**
		 * Gets the aql name.
		 *
		 * @return the aql name
		 */
		String getAqlName() {
			return aqlName;
		}
	}
	
	/**
	 * Create a new QueryBuilder with config of whether Collection Names should be prefixed with Graph name or not.
	 */
	public ArangoDBQueryBuilder( boolean shouldPrefixCollectionWithGraphName) {
		this.shouldPrefixWithGraphName = shouldPrefixCollectionWithGraphName;
		this.queryBuilder = new StringBuilder();
	}

	/**
	 * Append a WITH statement to the query builder for the given collections. The required bindVars are
	 * added to the bindVars map.
	 * @param graphName 			the graph name
	 * @param collections 			the list of Collections to use in the statement
	 * @param bindVars 				the map of bind parameters
	 *
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder with(
		String graphName,
		List<String> collections, Map<String, Object> bindVars) {
		queryBuilder.append("WITH ");
		String separator = "";
		int colId = 1;
		for (String c : collections) {
			queryBuilder.append(separator);
			separator = ",";
			String varName = String.format("@with%s", colId);
			queryBuilder.append("@").append(varName);
			bindVars.put(varName, ArangoDBUtil.getCollectioName(graphName, c, this.shouldPrefixWithGraphName));
		}
		queryBuilder.append("\n");
		logger.debug("with", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Append a Document and FILTER statements to the query builder. Use this to find a single or
	 * group of elements in the graph. This segment should be used in conjunction with the 
	 * {@link #with(String, List, Map)} segment.
	 *
	 * @param ids 					the id(s) to look for
	 * @param loopVariable 			the loop variable name
	 * @param bindVars	 			the map of bind parameters
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder documentsById(
		List<String> ids,
		String loopVariable,
		Map<String, Object> bindVars) {
		queryBuilder.append("LET docs = FLATTEN(RETURN Document(@ids))\n");
		queryBuilder.append(String.format("FOR %s IN docs\n", loopVariable));
		queryBuilder.append(String.format("  FILTER NOT IS_NULL(%s)\n", loopVariable)); // Not needed?
		bindVars.put("ids", ids);
		logger.debug("documentsById", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Append a Document statement to find a single element in the graph. This segment should be
	 * used in conjunction with the {@link #with(String, List, Map)} segment.
	 *
	 * @param id 					the id to look for
	 * @param loopVariable 			the loop variable name
	 * @param bindVars the 			the map of bind parameters
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder documentById(
		String id,
		String loopVariable,
		Map<String, Object> bindVars) {
		queryBuilder.append(String.format("LET %s = Document(@id)\n", loopVariable));
		bindVars.put("id", id);
		logger.debug("documentById", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Append an union segment.
	 * @param graphName 			the graph name
	 * @param collections 			the collections that participate in the union
	 * @param loopVariable 			the loop variable
	 * @param bindVars 				the map of bind parameters
	 *
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder union(
		String graphName,
		List<String> collections,
		String loopVariable,
		Map<String, Object> bindVars) {
		int count = 1;
		String separator = "";
		queryBuilder.append(String.format("FOR %s in UNION( \n", loopVariable));
		queryBuilder.append("  (");
		for (String c : collections) {
			queryBuilder.append(separator);
			separator = "),\n  (";
			queryBuilder.append(String.format("FOR x%1$s IN @@col%1$s RETURN x%1$s", count));
			bindVars.put(String.format("@col%s", count++), ArangoDBUtil.getCollectioName(graphName, c, shouldPrefixWithGraphName));
		}
		queryBuilder.append("  )\n");
		queryBuilder.append(")\n");
		logger.debug("union", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Add a FOR x IN y iteration to the query. A global collection counter is used so this operation
	 * can be used to created nested loops.
	 * @param graphName 			the graph name
	 * @param loopVariable 			the loop variable
	 * @param collectionName 		the collection name
	 * @param bindVars 				the map of bind parameters
	 *
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder iterateCollection(
		String graphName,
		String loopVariable,
		String collectionName, Map<String, Object> bindVars) {
		queryBuilder.append(String.format("FOR %1$s IN @@col%2$s", loopVariable, iterateCnt)).append("\n");
		bindVars.put(String.format("@col%s", iterateCnt++), ArangoDBUtil.getCollectioName(graphName, collectionName, shouldPrefixWithGraphName));
		logger.debug("iterateCollection", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Add a graph iteration segment.
	 * @param graphName 			the graph name
	 * @param vertexVariable 		the vertex variable
	 * @param edgeVariable 			the edge variable
	 * @param pathVariable 			the path variable
	 * @param min 					edges and vertices returned by this query will start at the traversal depth of min 
	 * @param max 					up to max length paths are traversed
	 * @param direction 			follow edges pointing in the direction
	 * @param startVertex 			the start vertex id
	 * @param bindVars 				the map of bind parameters
	 *
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder iterateGraph(
		String graphName,
		String vertexVariable,
		Optional<String> edgeVariable,
		Optional<String> pathVariable,
		Optional<Integer> min,
		Optional<Integer> max,
		Direction direction,
		String startVertex,
		Map<String, Object> bindVars) {
		queryBuilder.append(String.format("FOR %s", vertexVariable));
		if (edgeVariable.isPresent()) {
			queryBuilder.append(String.format(", %s", edgeVariable.get()));
		}
		if (pathVariable.isPresent()) {
			queryBuilder.append(String.format(", %s", pathVariable.get()));
		}
		queryBuilder.append("\n  IN ");
		if (min.isPresent()) {
			queryBuilder.append(min.get());
			if (max.isPresent()) {
				queryBuilder.append(String.format("..%s", max.get()));
			}
			queryBuilder.append(" ");
		}
		queryBuilder.append(direction.getAqlName()).append(" @startVertex\n")
			.append("    GRAPH '").append(graphName).append("'\n");		// FIXME Graph could be a parameter
		bindVars.put("startVertex", startVertex);
		logger.debug("iterateGraph", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Iterate over a collection of edges.
	 * @param graphName 			the graph name
	 * @param vertexVariable 		the vertex variable
	 * @param edgeVariable 			the edge variable
	 * @param pathVariable 			the path variable
	 * @param min 					edges and vertices returned by this query will start at the traversal depth of min 
	 * @param max 					up to max length paths are traversed
	 * @param direction 			follow edges pointing in the direction
	 * @param edgeCollections 		the edge collections
	 * @param startVertex 			the start vertex id
	 * @param bindVars 				the map of bind parameters
	 *
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder iterateEdges(
		String graphName,
		String vertexVariable,
		Optional<String> edgeVariable,
		Optional<String> pathVariable,
		Optional<Integer> min,
		Optional<Integer> max,
		Direction direction,
		List<String> edgeCollections,
		String startVertex, Map<String, Object> bindVars) {
		queryBuilder.append(String.format("FOR %s", vertexVariable));
		if (edgeVariable.isPresent()) {
			queryBuilder.append(String.format(", %s", edgeVariable.get()));
		}
		if (pathVariable.isPresent()) {
			queryBuilder.append(String.format(", %s", pathVariable.get()));
		}
		queryBuilder.append("\n  IN ");
		if (min.isPresent()) {
			queryBuilder.append(min.get());
		}
		if (max.isPresent()) {
			queryBuilder.append(String.format("..%s", max.get()));
		}
		queryBuilder.append(direction.getAqlName()).append(" @startVertex\n");
		String separator = "";
		for (String c : edgeCollections) {
			queryBuilder.append(separator);
			separator = ", ";
			queryBuilder.append(String.format("@@col%s", iterateCnt));
			bindVars.put(String.format("@col%s", iterateCnt++), ArangoDBUtil.getCollectioName(graphName, c, shouldPrefixWithGraphName));
		}
		bindVars.put("@startVertex", startVertex);
		logger.debug("iterateGraph", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Add a Graph options segment.
	 *
	 * @see UniqueVertices
	 * @see UniqueEdges
	 * @param onVertices 			the vertices options
	 * @param onEdges 				the edges options
	 * @param bfs 					if true, the traversal will be executed breadth-first, else it will
	 * 								be executed depth-first.
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder graphOptions(
		Optional<UniqueVertices> onVertices,
		Optional<UniqueEdges> onEdges,
		boolean bfs) {
		if (onVertices.isPresent() || onEdges.isPresent() || bfs) {
			queryBuilder.append("  OPTIONS {");
			if (onVertices.isPresent()) {
				queryBuilder.append(String.format("uniqueVertices: '%s', ", onVertices.get().getAqlName()));
			}
			if (onEdges.isPresent()) {
				queryBuilder.append(String.format("uniqueEdges: '%s', ", onEdges.get().getAqlName()));
			}
			if (bfs) {
				queryBuilder.append("bfs: true");
			}
			queryBuilder.append("}\n");
		}
		logger.debug("graphOptions", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Add a filter same collections segment, i.e. element represented by variable must be in any
	 * of the provided collections.
	 * @param graphName 			the graph name
	 * @param filterVariable 		the filter variable
	 * @param collections 			the collections to filter by
	 * @param bindVars 				the map of bind parameters
	 *
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder filterSameCollections(
		String graphName,
		String filterVariable,
		List<String> collections, Map<String, Object> bindVars) {
		if (!collections.isEmpty()) {
			queryBuilder.append(" FILTER (IS_SAME_COLLECTION(");
			String separator = "";
			for (String c : collections) {
				queryBuilder.append(separator);
				separator = String.format(", %s) OR IS_SAME_COLLECTION(", filterVariable);
				queryBuilder.append(String.format("@@col%s", iterateCnt));
				bindVars.put(String.format("@col%s", iterateCnt++), ArangoDBUtil.getCollectioName(graphName, c, shouldPrefixWithGraphName));
			}
			queryBuilder.append(String.format(", %s))\n", filterVariable));
		}
		filtered = true;
		logger.debug("filterSameCollections", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Add a filter on element properties segment. The filter operations are defined using a 
	 * #link {@link ArangoDBPropertyFilter}.
	 *
	 * @param propertyFilter		the property filter
	 * @param filterVariable 		the filter variable
	 * @param bindVars 				the map of bind parameters
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder filterProperties(
		ArangoDBPropertyFilter propertyFilter,
		String filterVariable,
		Map<String, Object> bindVars) {
		List<String> filterSegments = new ArrayList<String>();
		propertyFilter.addAqlSegments(String.format("%s.", filterVariable), filterSegments, bindVars);
		if (CollectionUtils.isNotEmpty(filterSegments)) {
			if (filtered) {
				queryBuilder.append(" AND ");
			} else {
				queryBuilder.append(" FILTER ");
            }
			queryBuilder.append(StringUtils.join(filterSegments, " AND ")).append("\n");
		}
		logger.debug("filterProperties", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Add a limit segment to limit the number of elements returned.
	 *
	 * @param limit 				the limit number
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder limit(Long limit) {
		queryBuilder.append(" LIMIT " + limit.toString());
		logger.debug("limit", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Add a RETURN Segment. 
	 * TODO provide finer grained return statements 
	 *
	 * @param returnStatement the return statement
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder ret(String returnStatement) {
		queryBuilder.append("RETURN ").append(returnStatement).append("\n");
		logger.debug("ret", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Appends the specified string to this character sequence.
	 * <p>
	 * The characters of the String argument are appended, in order, increasing the length of this 
	 * sequence by the length of the argument. If str is null, then the four characters "null" are
	 * appended.
	 * <p>
	 * Let n be the length of this character sequence just prior to execution of the append method.
	 * Then the character at index k in the new character sequence is equal to the character at
	 * index k in the old character sequence, if k is less than n; otherwise, it is equal to the 
	 * character at index k-n in the argument str.
	 * @param segment	a str
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder append(String segment) {
		queryBuilder.append(segment);
		return this;
	}
	
	@Override
	public String toString() {
		return queryBuilder.toString();
	}

}
