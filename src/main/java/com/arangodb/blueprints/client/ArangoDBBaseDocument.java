//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints.client;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;

import com.arangodb.ErrorNums;
import com.google.gson.Gson;

/**
 * The ArangoDB document class
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

	protected Map<String, Object> properties;

	/**
	 * true if the document is deleted
	 */

	protected boolean deleted = false;

	/**
	 * Returns all document properties as a JSON object
	 * 
	 * @return the JSON object
	 */
	public Map<String, Object> getProperties() {
		return properties;
	}

	/**
	 * Sets the document status to "deleted"
	 */
	public void setDeleted() {
		properties = new TreeMap<String, Object>();
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
	public void setProperties(Map<String, Object> properties) throws ArangoDBException {
		if (properties != null) {
			this.properties = new TreeMap<String, Object>(properties);
		} else {
			this.properties = new TreeMap<String, Object>();
		}
		checkStdProperties();
	}

	protected void checkHasProperty(String name) throws ArangoDBException {
		if (!properties.containsKey(name)) {
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
		return this.properties.get(key);
	}

	/**
	 * Returns property keys
	 * 
	 * @return the property keys
	 */
	public Set<String> getPropertyKeys() {
		Set<String> result = new HashSet<String>();

		for (Map.Entry<String, Object> entry : properties.entrySet()) {
			String key = entry.getKey();
			if (key.charAt(0) != '_' && key.charAt(0) != '$') {
				result.add(key);
			}
		}

		return result;
	}

	/**
	 * Checks the document values and returns converted values. Supported value
	 * types: Boolean, Integer, Long, String, Double, Float, Map, List
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
		} else if (value instanceof Integer) {
			return new Double(((Integer) value).doubleValue());
		} else if (value instanceof Long) {
			return new Double(((Long) value).doubleValue());
		} else if (value instanceof Float) {
			return new Double(((Float) value).doubleValue());
		} else if (value instanceof String) {
			return value;
		} else if (value instanceof Double) {
			return value;
		} else if (value instanceof Map) {
			Map<String, Object> result = new TreeMap<String, Object>();
			Map<?, ?> m = (Map<?, ?>) value;

			for (Map.Entry<?, ?> entry : m.entrySet()) {
				if (entry.getKey() instanceof String) {
					result.put((String) entry.getKey(), toDocumentValue(entry.getValue()));
				} else {
					throw new ArangoDBException("a key of a Map has to be a String",
							ErrorNums.ERROR_GRAPH_INVALID_PARAMETER);
				}
			}

			return result;

		} else if (value instanceof List) {
			List<?> l = (List<?>) value;

			for (final Object k : l) {
				toDocumentValue(k);
			}

			return value;
		} else {
			// TODO add more types

			throw new ArangoDBException("class of value not supported: " + value.getClass().toString(),
					ErrorNums.ERROR_GRAPH_INVALID_PARAMETER);
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
		if (StringUtils.isNotBlank(key)) {
			if (key.charAt(0) != '_' && key.charAt(0) != '$') {
				properties.put(key, toDocumentValue(value));
			} else {
				throw new ArangoDBException("property key is reserved");
			}
		} else {
			throw new ArangoDBException("property value cannot be empty");
		}
	}

	protected void setSystemProperty(String key, Object value) throws ArangoDBException {
		if (StringUtils.isNotBlank(key)) {
			properties.put(key, toDocumentValue(value));
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
		if (StringUtils.isNotBlank(key)) {
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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (deleted ? 1231 : 1237);
		result = prime * result + ((properties == null) ? 0 : serializeProperties(properties).hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ArangoDBBaseDocument other = (ArangoDBBaseDocument) obj;
		if (deleted != other.deleted)
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!serializeProperties(properties).equals(serializeProperties(other.properties))) {
			return false;
		}
		return true;
	}

	private String serializeProperties(Map<String, Object> map) {
		Gson gson = new Gson();
		try {
			return gson.toJson(map);
		} catch (Exception e) {
			return "";
		}
	}
}
