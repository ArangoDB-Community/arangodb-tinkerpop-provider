//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ArangoDBBaseCursor {

    /**
     * the ArangoDB client
     *
     */

	private ArangoDBSimpleGraphClient client;
	
    /**
     * the cached results
     *
     */

	private JSONArray result = null;

    /**
     * has more results property 
     *
     */

	private boolean hasNext = false;
	
    /**
     * cursor identifier 
     *
     */

	private String id = "";
	
    /**
     * next result 
     *
     */

	private int index = 0; 
	
    /**
     * count result 
     *
     */

	private int count = 0; 
		
    /**
     * Create a ArangoDB cursor
     *
     * @param client       the ArangoDB client
     * @param json         the result of a query in cursor structure 
     */

	public ArangoDBBaseCursor (ArangoDBSimpleGraphClient client, JSONObject json) {
		this.client = client;
		setValues(json);
	}
	
    /**
     * returns true, if there a more results
     *  
     * @return true, if there are more results 
     */

	public boolean hasNext () {
		return hasNext || index < result.length();
	}
	
    /**
     * returns the next result
     *  
     * @return Object 
     */

	public Object next () {
		if (hasNext()) {			
			if (index >= result.length()) {
				// load next chunk
				if (!update()) {
					return null;
				}
			}
			
			try {
				return result.get(index++);
			} catch (JSONException e) {
			}
		}
		
		return null;
	}
	
    /**
     * loads more results and updates the state
     */

	private boolean update () {		
		try {
			JSONObject json = client.getNextCursorValues(id);
			return setValues(json);
			
		} catch (ArangoDBException e) {
			return false;
		}		
	}
		
    /**
     * updates the state
     *  
     * @param json         json in cursor data structure 
     * @return true,       if the state was updated successfully 
     */

	private boolean setValues (JSONObject json) {
		if (json == null) {
			result = new JSONArray();
			hasNext = false;
			id = "";
			index = 0;
			count = 0;
			return false;
		}
		
		if (json.has("result")) {
			try {
				result = json.getJSONArray("result");
			} catch (JSONException e) {
				result = new JSONArray();
			}
		}
		if (json.has("hasMore")) {
			try {
				hasNext = json.getBoolean("hasMore");
			} catch (JSONException e) {
				hasNext = false;
			}			
		}
		if (json.has("id")) {
			try {
				id = json.getString("id");
			} catch (JSONException e) {
				id = "";
				hasNext = false;
			}			
		}
		if (json.has("count")) {
			try {
				count = json.getInt("count");
			} catch (JSONException e) {
				count = 0;
			}			
		}
		
		if (result == null) {
			result = new JSONArray();
		}
		
		index = 0;
		return true;
	}
	
    /**
     * deletes the cursor on the server
     */
	
	public void close () {
		if (!hasNext()) {
			return;			
		}
		
		client.deleteCursor(id);
		
		result = new JSONArray();
		hasNext = false;
		id = "";
		index = 0;
	}
	
	public int count() {
		return this.count;
	}
}
