//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import org.codehaus.jettison.json.JSONObject;

/**
 * The arangodb vertex class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

public class ArangoDBSimpleVertex extends ArangoDBBaseDocument {

	/**
	 * Creates a new vertex by a JSON document
	 * 
	 * @param properties
	 *            The JSON document
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */

	public ArangoDBSimpleVertex(JSONObject properties) throws ArangoDBException {
		this.properties = properties;
		checkStdProperties();
	}

}
