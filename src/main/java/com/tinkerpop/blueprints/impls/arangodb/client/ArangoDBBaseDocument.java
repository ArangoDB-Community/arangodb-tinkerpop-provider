//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONString;

/**
 * The arangodb document class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

abstract public class ArangoDBBaseDocument {

	/**
	 * document id attribute
	 */

	public static final String _ID = "_id";

	/**
	 * document revision attribute
	 */

	public static final String _REV = "_rev";

	/**
	 * document key attribute
	 */

	public static final String _KEY = "_key";

	/**
	 * all document properties
	 */

	protected JSONObject properties;

	/**
	 * true if the document is deleted
	 */

	protected boolean deleted = false;

	/**
	 * Returns all document properties as a JSON object
	 * 
	 * @return the JSON object
	 */
	public JSONObject getProperties() {
		return properties;
	}

	/**
	 * Sets the document status to "deleted"
	 */
	public void setDeleted() {
		properties = new JSONObject();
		deleted = true;
	}

	/**
	 * Returns true if the document has status "deleted"
	 * 
	 * @return true, if the document is deleted
	 */
	public boolean isDeleted() {
		return deleted;
	}

	/**
	 * Sets all document properties by a JSON object
	 * 
	 * @param properties
	 *            the JSON object
	 * 
	 * @throws ArangoDBException
	 *             If the type is not supported
	 */
	public void setProperties(JSONObject properties) throws ArangoDBException {
		this.properties = properties;
		checkStdProperties();
	}

	protected void checkHasProperty(String name) throws ArangoDBException {
		if (!properties.has(name)) {
			throw new ArangoDBException("Missing property '" + name + "'");
		}
	}

	protected void checkStdProperties() throws ArangoDBException {
		checkHasProperty(_ID);
		checkHasProperty(_REV);
		checkHasProperty(_KEY);
	}

	/**
	 * Returns a property value
	 * 
	 * @param key
	 *            The key of the property
	 * 
	 * @return the property value
	 */
	public Object getProperty(String key) {
		try {
			return this.properties.get(key);
		} catch (JSONException e) {
			return null;
		}
	}

	/**
	 * Returns property keys
	 * 
	 * @return the property keys
	 */
	public Set<String> getPropertyKeys() {
		Set<String> result = new HashSet<String>();

		Iterator<?> iter = this.properties.keys();
		while (iter.hasNext()) {
			String key = iter.next().toString();
			if (key.charAt(0) != '_' && key.charAt(0) != '$') {
				result.add(key);
			}
		}

		return result;
	}

	/**
	 * Checks the document values and returns converted values. Supported value
	 * types: Boolean, Integer, Long, String, Double, Float, JSONArray,
	 * JSONObject, JSONString, Map, List
	 * 
	 * @param value
	 *            a value of an attribute
	 * @return Object a supported (or converted) value
	 * 
	 * @throws ArangoDBException
	 *             If the type is not supported
	 */
	public static Object toDocumentValue(Object value) throws ArangoDBException {
		if (value instanceof Boolean) {
			return value;
		} else if (value instanceof Integer || value instanceof Long) {
			return value;
		} else if (value instanceof String) {
			return value;
		} else if (value instanceof Double || value instanceof Float) {
			return value;
		} else if (value instanceof JSONArray || value instanceof JSONObject || value instanceof JSONString) {
			return value;
		} else if (value instanceof Map) {
			Map m = (Map) value;
			Set set = m.keySet();

			JSONObject obj = new JSONObject();

			for (final Object k : set) {
				if (k instanceof String) {
					String key = (String) k;
					try {
						obj.put(key, toDocumentValue(m.get(key)));
					} catch (JSONException e) {
						throw new ArangoDBException("class of value not supported: " + m.get(key).getClass().toString()
								+ ", " + e.getMessage());
					}
				} else {
					throw new ArangoDBException("a key of a Map has be a String");
				}
			}

			return obj;

		} else if (value instanceof List) {
			List l = (List) value;

			JSONArray obj = new JSONArray();

			for (final Object k : l) {
				obj.put(toDocumentValue(k));
			}

			return obj;
		} else {
			// TODO add more types

			throw new ArangoDBException("class of value not supported: " + value.getClass().toString());
		}

	}

	/**
	 * Set a single property value
	 * 
	 * @param key
	 *            the property key
	 * @param value
	 *            the property value
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public void setProperty(String key, Object value) throws ArangoDBException {
		if (key != null && key.length() > 0) {

			if (key.charAt(0) != '_' && key.charAt(0) != '$') {
				try {
					properties.put(key, toDocumentValue(value));
				} catch (JSONException e) {
					throw new ArangoDBException("cannot set property. " + e.getMessage());
				}
			} else {
				throw new ArangoDBException("property key is reserved");
			}
		} else {
			throw new ArangoDBException("property value cannot be empty");
		}
	}

	protected void setSystemProperty(String key, Object value) throws ArangoDBException {
		if (key != null && key.length() > 0) {
			try {
				properties.put(key, toDocumentValue(value));
			} catch (JSONException e) {
				throw new ArangoDBException("cannot set property");
			}
		} else {
			throw new ArangoDBException("property value cannot be empty");
		}
	}

	/**
	 * Removes a single property
	 * 
	 * @param key
	 *            the key of the property
	 * 
	 * @return the value of the removed property
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public Object removeProperty(String key) throws ArangoDBException {
		if (key != null && key.length() > 0) {
			if (key.charAt(0) != '_' && key.charAt(0) != '$') {
				return properties.remove(key);
			} else {
				throw new ArangoDBException("property key is reserved");
			}
		}
		return null;
	}

	protected String getStringProperty(String key) {
		Object o = getProperty(key);
		if (o == null) {
			return "";
		}

		return o.toString();
	}

	/**
	 * Returns the document identifier
	 * 
	 * @return the document identifier
	 */
	public String getDocumentId() {
		return getStringProperty(_ID);
	}

	/**
	 * Returns the document version
	 * 
	 * @return the document version
	 */
	public String getDocumentRev() {
		return getStringProperty(_REV);
	}

	/**
	 * Returns the document long identifier
	 * 
	 * @return the document long identifier
	 */
	public String getDocumentKey() {
		return getStringProperty(_KEY);
	}

	public String toString() {
		if (properties == null) {
			return "null";
		}
		return properties.toString();
	}

	@Override
	public boolean equals(Object other) {

		if (other == null)
			return false;
		if (other == this)
			return true;
		if (other.getClass() != getClass())
			return false;

		ArangoDBBaseDocument document = (ArangoDBBaseDocument) other;

		return properties.toString().equals(document.getProperties().toString());
	}

}
