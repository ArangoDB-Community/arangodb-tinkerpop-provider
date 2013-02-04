package com.tinkerpop.blueprints.impls.arangodb.client;

import java.util.Vector;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ArangoDBIndex {

	/**
	 * id of the index
	 */

	private String id;

	/**
	 * the index type
	 */

	private String type;
	
	/**
	 * is the index unique
	 */

	private boolean unique;
	
	/**
	 * the fields of the index
	 */

	private Vector<String> fields = new Vector<String>();
	
	public ArangoDBIndex (JSONObject json) throws ArangoDBException {
		if (json == null) {
			throw new ArangoDBException("JSON data for index is empty.");
		}
		
		try {
			if (json.has("id")) {
				this.id = json.getString("id");
			}
			else {
				throw new ArangoDBException("JSON data for index has no 'id' attribute.");
			}
			if (json.has("type")) {
				this.type = json.getString("type");
			}
			else {
				throw new ArangoDBException("JSON data for index has no 'type' attribute.");				
			}
			if (json.has("unique")) {
				this.unique = json.getBoolean("unique");
			}
			else {
				throw new ArangoDBException("JSON data for index has no 'unique' attribute.");				
			}
			if (json.has("fields")) {
				JSONArray a = json.getJSONArray("fields");
				for (int i = 0; i < a.length(); i++) {
					fields.add(a.getString(i));
				}
			}
			else {
				throw new ArangoDBException("JSON data for index has no 'fields' attribute.");				
			}
			
		} catch (JSONException e) {
			throw new ArangoDBException("Error in JSON data. " + e.getMessage());
		}			
	}

	public ArangoDBIndex (String id, String type, boolean unique, Vector<String> fields) {
		this.id = id;
		this.type = type;
		this.unique = unique;
		this.fields = new Vector<String>(fields);				
	}
	
	public String getId() {
		return id;
	}

	public String getType() {
		return type;
	}

	public boolean isUnique() {
		return unique;
	}

	public Vector<String> getFields() {
		return fields;
	}
	
}
