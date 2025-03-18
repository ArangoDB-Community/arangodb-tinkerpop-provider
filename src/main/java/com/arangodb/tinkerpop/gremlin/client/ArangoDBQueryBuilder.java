//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.util.List;
import java.util.Map;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ArangoDB Query Builder class provides static methods for building AQL fragments that can be concatenated to build
 * complete AQL queries. Note that all parameters used to create query fragments are used as is, hence, all
 * pre-processing (e.g. prefix collection names) must be done in the callee.
 *
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public class ArangoDBQueryBuilder {
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBQueryBuilder.class);
	
	private StringBuilder queryBuilder;
	
	private int iterateCnt = 1;

	private boolean filtered = false;

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
		
	}
	
	/**
	 * Create a new QueryBuilder with config of whether Collection Names should be prefixed with Graph name or not.
	 */
	public ArangoDBQueryBuilder() {
		this.queryBuilder = new StringBuilder();
	}

	/**
	 * Append a WITH statement to the query builder for the given collections. The required bindVars are
	 * added to the bindVars map.
	 * @param collections 			the list of Collections to use in the statement
	 * @param bindVars 				the map of bind parameters
	 *
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder with(List<String> collections, Map<String, Object> bindVars) {
		queryBuilder.append("WITH ");
		String separator = "";
		int colId = 1;
		for (String c : collections) {
			queryBuilder.append(separator);
			separator = ",";
			String varName = String.format("@with%s", colId);
			queryBuilder.append("@").append(varName);
			bindVars.put(varName, c);
		}
		queryBuilder.append("\n");
		logger.debug("with", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Append a Document and FILTER statements to the query builder. Use this to find a single or
	 * group of elements in the graph. This segment should be used in conjunction with the 
	 * {@link #with(List, Map)} segment.
	 *
	 * @param ids 					the id(s) to look for
	 * @param loopVariable 			the loop variable name
	 * @param bindVars	 			the map of bind parameters
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder documentsById(
		List<ArangoDBId> ids,
		String loopVariable,
		Map<String, Object> bindVars) {
		queryBuilder.append("LET docs = FLATTEN(RETURN Document(@ids))\n");
		queryBuilder.append(String.format("FOR %s IN docs\n", loopVariable));
		queryBuilder.append(String.format("  FILTER NOT IS_NULL(%s)\n", loopVariable)); // Not needed?
		bindVars.put("ids", ids);
		logger.debug("documentsById: {}", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Append an union segment.
	 * @param collections 			the collections that participate in the union
	 * @param loopVariable 			the loop variable
	 * @param bindVars 				the map of bind parameters
	 *
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder union(
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
			bindVars.put(String.format("@col%s", count++), c);
		}
		queryBuilder.append("  )\n");
		queryBuilder.append(")\n");
		logger.debug("union", queryBuilder.toString());
		return this;
	}
	
	/**
	 * Add a FOR x IN y iteration to the query. A global collection counter is used so this operation
	 * can be used to created nested loops.
	 * @param loopVariable 			the loop variable
	 * @param collectionName 		the collection name
	 * @param bindVars 				the map of bind parameters
	 *
	 * @return a reference to this object.
	 */
	
	public ArangoDBQueryBuilder iterateCollection(
		String loopVariable,
		String collectionName, Map<String, Object> bindVars) {
		queryBuilder.append(String.format("FOR %1$s IN @@col%2$s", loopVariable, iterateCnt)).append("\n");
		bindVars.put(String.format("@col%s", iterateCnt++), collectionName);
		logger.debug("iterateCollection", queryBuilder.toString());
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
