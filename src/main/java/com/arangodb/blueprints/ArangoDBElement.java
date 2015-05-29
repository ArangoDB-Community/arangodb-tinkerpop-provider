package com.arangodb.blueprints;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.arangodb.blueprints.client.ArangoDBBaseDocument;
import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.utils.ArangoDBUtil;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * The ArangoDB base element class (used by edges and vertices)
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

abstract public class ArangoDBElement implements Element {

	/**
	 * the graph of the document
	 */

	protected ArangoDBGraph graph;

	/**
	 * the vertex/edge document
	 */

	protected ArangoDBBaseDocument document;

	/**
	 * Save the vertex or the edge in ArangoDB
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */

	abstract public void save() throws ArangoDBException;

	/**
	 * Return the object value associated with the provided string key. If no
	 * value exists for that key, return null.
	 * 
	 * @param key
	 *            the key of the key/value property
	 * @return the object value related to the string key
	 */
	@SuppressWarnings("unchecked")
	public <T> T getProperty(String key) {
		return (T) document.getProperty(ArangoDBUtil.normalizeKey(key));
	}

	/**
	 * Set/Reset the vertex/edge document
	 * 
	 * @param document
	 *            the new internal data of the element
	 */
	public void setDocument(ArangoDBBaseDocument document) {
		this.document = document;
	}

	public Set<String> getPropertyKeys() {
		Set<String> ps = document.getPropertyKeys();
		HashSet<String> result = new HashSet<String>();

		if (this instanceof Edge) {
			// do not return lable property
			for (String key : ps) {
				if (!StringFactory.LABEL.equals(key)) {
					result.add(ArangoDBUtil.denormalizeKey(key));
				}
			}
		} else {
			for (String key : ps) {
				result.add(ArangoDBUtil.denormalizeKey(key));
			}
		}
		return result;
	}

	public void setProperty(String key, Object value) {

		if (StringFactory.ID.equals(key)) {
			throw ExceptionFactory.propertyKeyIdIsReserved();
		}

		if (StringFactory.LABEL.equals(key) && this instanceof Edge) {
			throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
		}

		if (StringUtils.isBlank(key)) {
			throw ExceptionFactory.propertyKeyCanNotBeEmpty();
		}

		try {
			document.setProperty(ArangoDBUtil.normalizeKey(key), value);
			save();
		} catch (ArangoDBException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T removeProperty(String key) {

		if (StringUtils.isBlank(key)) {
			throw ExceptionFactory.propertyKeyCanNotBeEmpty();
		}

		if (key.equals(StringFactory.LABEL) && this instanceof Edge) {
			throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
		}

		T o = null;
		try {
			o = (T) document.removeProperty(ArangoDBUtil.normalizeKey(key));
			save();

		} catch (ArangoDBException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		return o;
	}

	public Object getId() {
		return document.getDocumentKey();
	}

	/**
	 * Returns the internal data of the element
	 * 
	 * @return the internal data
	 */
	public ArangoDBBaseDocument getRaw() {
		return document;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((document == null) ? 0 : document.hashCode());
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
		ArangoDBElement other = (ArangoDBElement) obj;
		if (document == null) {
			if (other.document != null)
				return false;
		} else if (!document.equals(other.document))
			return false;
		return true;
	}

}
