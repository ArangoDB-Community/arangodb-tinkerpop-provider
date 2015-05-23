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
	 *            The properties
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */

	public ArangoDBSimpleVertex(Map<String, Object> properties) throws ArangoDBException {
		this.setProperties(properties);
	}

}
