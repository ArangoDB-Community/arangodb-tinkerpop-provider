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
 * The arangodb vertex cursor class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */
public class ArangoDBSimpleVertexCursor extends ArangoDBBaseCursor {

	/**
	 * Creates an vertex cursor
	 * 
	 * @param client
	 *            The graph client
	 * @param json
	 *            The initial JSON document
	 */
	public ArangoDBSimpleVertexCursor(ArangoDBSimpleGraphClient client, JSONObject json) {
		super(client, json);
	}

	public ArangoDBSimpleVertex next() {
		Object o = super.next();

		if (o == null) {
			return null;
		}

		if (o instanceof JSONObject) {
			try {
				return new ArangoDBSimpleVertex((JSONObject) o);
			} catch (ArangoDBException e) {
			}
		}

		return null;
	}

}
