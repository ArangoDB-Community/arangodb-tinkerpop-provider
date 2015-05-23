//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints.client;

import java.util.Map;

/**
 * The ArangoDB simple edge class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBSimpleEdge extends ArangoDBBaseDocument {

	/**
	 * the name of the "to" attribute
	 */
	public static final String _TO = "_to";

	/**
	 * the name of the "from" attribute
	 */
	public static final String _FROM = "_from";

	/**
	 * the name of the "label" attribute
	 */
	public static final String _LABEL = "$label";

	/**
	 * Creates a new edge by a JSON document
	 * 
	 * @param properties
	 *            The JSON document
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public ArangoDBSimpleEdge(Map<String, Object> properties) throws ArangoDBException {
		this.setProperties(properties);
		checkHasProperty(_TO);
		checkHasProperty(_FROM);
	}

	/**
	 * Returns the edge name
	 * 
	 * @return the edge name
	 */
	public String getName() {
		return getDocumentKey();
	}

	/**
	 * Returns the identifier of the "to" vertex
	 * 
	 * @return the identifier of the "to" vertex
	 */
	public String getToVertexId() {
		return getStringProperty(_TO);
	}

	/**
	 * Returns the identifier of the "from" vertex
	 * 
	 * @return the identifier of the "from" vertex
	 */
	public String getFromVertexId() {
		return getStringProperty(_FROM);
	}

	/**
	 * Returns the edge label
	 * 
	 * @return the edge label
	 */
	public String getLabel() {
		return getStringProperty(_LABEL);
	}

}
