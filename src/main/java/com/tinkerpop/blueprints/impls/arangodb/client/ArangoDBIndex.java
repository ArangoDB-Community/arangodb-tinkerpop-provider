package com.tinkerpop.blueprints.impls.arangodb.client;

import java.util.Vector;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * The arangodb index class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

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

	/**
	 * Creates an index by a given JSON document
	 * 
	 * @param json
	 *            The JSON document of the index
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public ArangoDBIndex(JSONObject json) throws ArangoDBException {
		if (json == null) {
			throw new ArangoDBException("JSON data for index is empty.");
		}

		try {
			if (json.has("id")) {
				this.id = json.getString("id");
			} else {
				throw new ArangoDBException("JSON data for index has no 'id' attribute.");
			}
			if (json.has("type")) {
				this.type = json.getString("type");
			} else {
				throw new ArangoDBException("JSON data for index has no 'type' attribute.");
			}
			if (json.has("unique")) {
				this.unique = json.getBoolean("unique");
			} else {
				throw new ArangoDBException("JSON data for index has no 'unique' attribute.");
			}
			if (json.has("fields")) {
				JSONArray a = json.getJSONArray("fields");
				for (int i = 0; i < a.length(); i++) {
					fields.add(a.getString(i));
				}
			} else {
				throw new ArangoDBException("JSON data for index has no 'fields' attribute.");
			}

		} catch (JSONException e) {
			throw new ArangoDBException("Error in JSON data. " + e.getMessage());
		}
	}

	/**
	 * Creates a new index
	 * 
	 * @param id
	 *            The index identifier
	 * @param type
	 *            The index type
	 * @param unique
	 *            True, if the index should be unique
	 * @param fields
	 *            The index fields
	 */
	public ArangoDBIndex(String id, String type, boolean unique, Vector<String> fields) {
		this.id = id;
		this.type = type;
		this.unique = unique;
		this.fields = new Vector<String>(fields);
	}

	/**
	 * Returns the index identifier
	 * 
	 * @return the index identifier
	 */
	public String getId() {
		return id;
	}

	/**
	 * Returns the index type
	 * 
	 * @return the index type
	 */
	public String getType() {
		return type;
	}

	/**
	 * Returns true if the index is unique
	 * 
	 * @return true, if the index is unique
	 */
	public boolean isUnique() {
		return unique;
	}

	/**
	 * Returns the list of fields
	 * 
	 * @return the list of fields
	 */
	public Vector<String> getFields() {
		return fields;
	}

}
