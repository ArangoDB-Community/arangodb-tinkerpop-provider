package com.arangodb.tinkerpop.gremlin.batch;

import java.util.HashSet;
import java.util.Set;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import org.apache.log4j.Logger;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBException;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * The arangodb base batch element class (used by edges and vertices)
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

abstract class ArangoDBBatchElement implements Element {

	/**
	 * the logger
	 */
	private static final Logger logger = Logger.getLogger(ArangoDBBatchElement.class);

	/**
	 * the graph of the document
	 */

	protected ArangoDBBatchGraph graph;

	/**
	 * the vertex/edge document
	 */

	protected ArangoDBBaseDocument document;

	/**
	 * true if the element was changed
	 */

	protected boolean created = false;

	/**
	 * Save the vertex or the edge in ArangoDB
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */

	public abstract void save() throws ArangoDBException;

	/**
	 * Return the object value associated with the provided string key. If no
	 * value exists for that key, return null.
	 * 
	 * @param key
	 *            the key of the key/value property
	 * @return the object value related to the string key
	 */
	@Override
	public Object getProperty(String key) {
		return document.getProperty(ArangoDBUtil.normalizeKey(key));
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

	@Override
	public Set<String> getPropertyKeys() {
		Set<String> ps = document.getPropertyKeys();
		HashSet<String> result = new HashSet<String>();
		for (String key : ps) {
			result.add(ArangoDBUtil.denormalizeKey(key));
		}
		return result;
	}

	@Override
	public void setProperty(String key, Object value) {
		if (created) {
			throw new UnsupportedOperationException();
		}

		if (isEmptyString(key))
			throw ExceptionFactory.propertyKeyCanNotBeEmpty();
		if (key.equals(StringFactory.ID))
			throw ExceptionFactory.propertyKeyIdIsReserved();

		try {
			document.setProperty(ArangoDBUtil.normalizeKey(key), value);
		} catch (ArangoDBException e) {
			logger.warn("could not set property", e);
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	@Override
	public Object removeProperty(String key) {
		checkRemovePropertyKey(key);

		Object o = null;
		try {
			o = document.removeProperty(ArangoDBUtil.normalizeKey(key));
		} catch (ArangoDBException e) {
			logger.warn("could not remove property", e);
			throw new IllegalArgumentException(e.getMessage());
		}
		return o;
	}

	private void checkRemovePropertyKey(String key) {
		if (created) {
			throw new UnsupportedOperationException();
		}

		if (isEmptyString(key))
			throw ExceptionFactory.propertyKeyCanNotBeEmpty();
		if (key.equals(StringFactory.ID))
			throw ExceptionFactory.propertyKeyIdIsReserved();
		if (key.equals(StringFactory.LABEL) && this instanceof Edge)
			throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
	}

	private boolean isEmptyString(String key) {
		return key == null || key.equals(StringFactory.EMPTY_STRING);
	}

	@Override
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

}
