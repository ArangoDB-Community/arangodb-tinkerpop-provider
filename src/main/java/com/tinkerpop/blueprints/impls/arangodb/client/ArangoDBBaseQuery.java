//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ArangoDBBaseQuery {

	/**
	 * the ArangoDB client
	 *
	 */
	
	protected ArangoDBSimpleGraphClient client;
	
	/**
	 * the cached results
	 *
	 */
	
	protected JSONObject queryBody;
		
	/**
	 * the request url
	 *
	 */
	
	protected String requestPath;
	
	/**
	 * Direction
	 *
	 */
	
    public enum Direction {IN, OUT, ALL};
	
	/**
	 * Create a simple query 
	 *
	 * @param client           a simple client
	 * @param requestPath      the http request path 
	 * @param propertyFilter   a property filter (or null)
	 * @param labelsFilter     a list of edge labels (or null)
	 * @param direction        a direction of labels (default "all")
	 * @param limit            limit the number of results (default "null")
	 * @param count            return the number of results
	 *
	 * @throws ArangoDBException    if an error occurs
	 */
	
	public ArangoDBBaseQuery (
			ArangoDBSimpleGraphClient client,
			String requestPath,
			ArangoDBPropertyFilter propertyFilter,
			List<String> labelsFilter,
			Direction direction,
			Long limit,
			boolean count) throws ArangoDBException {
		
		this.client = client;
		this.requestPath = requestPath;
		
		queryBody = new JSONObject();
		
		try {
			queryBody.put("batchSize", this.client.getConfiguration().getBatchSize());
			
			JSONObject filter = new JSONObject();
			
			if (propertyFilter != null) {				
				filter.put("properties", propertyFilter.getAsJSON());				
			}
			
			if (direction != null) {
				if (direction == Direction.IN) {
	            	filter.put("direction", "in");					
				}
				else if (direction == Direction.OUT) {
	            	filter.put("direction", "out");					
				}
			}
			
			if (labelsFilter != null && labelsFilter.size() > 0) {
				JSONArray a = new JSONArray();
				for (final String label : labelsFilter) {
					a.put(label);
				}
				filter.put("labels", a);
			}
						
			if (limit != null && limit.longValue() >= 0) {				
				queryBody.put("limit", limit.longValue());				
			}

			queryBody.put("count", count);			
			
			queryBody.put("filter", filter);			
		} catch (JSONException e) {
			e.printStackTrace();
			throw new ArangoDBException("JSON error: " + e.getMessage());
		}		
	}
	
	/**
	 * Executes the query and returns the result in a cursor
	 *
	 * @return ArangoDBBaseCursor   the result cursor
	 * @throws ArangoDBException    if the query could not be executed
	 */
	
	public ArangoDBBaseCursor getResult() throws ArangoDBException {
		
		JSONObject json = client.postRequest(requestPath, queryBody);
		
		return new ArangoDBBaseCursor(client, json);
	}
	
}
